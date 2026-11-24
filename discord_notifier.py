"""
Optional Discord webhook notifications for Google Takeout downloads.

Set ``DISCORD_WEBHOOK_URL`` to stream an embed to Discord whenever a download
starts, an archive completes, an archive fails, authentication expires, or the
run stops/finishes.

Notifications are queued and sent by a daemon thread, so an unreachable or
rate-limited webhook can never stall the download workers. Discord allows five
requests per five seconds per webhook; a small delay between messages keeps
bursts (for example, many small archives finishing at once) within that limit.
"""

import logging
import os
import queue
import re
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, Mapping, Optional

import requests

WEBHOOK_ENV = 'DISCORD_WEBHOOK_URL'
USERNAME = 'Takeout Downloader'

# Discord's per-webhook limit is 5 requests / 5 seconds. Leave headroom.
MIN_SEND_INTERVAL = 1.25
REQUEST_TIMEOUT = 10.0
MAX_RETRIES = 3
MAX_QUEUE_SIZE = 500

COLOR_INFO = 0x7C3AED
COLOR_SUCCESS = 0x22C55E
COLOR_WARNING = 0xF59E0B
COLOR_ERROR = 0xEF4444

logger = logging.getLogger(__name__)

# Error strings from requests can include the full signed download URL.
_URL_RE = re.compile(r'https?://[^\s\'"<>]+', re.IGNORECASE)


def format_bytes(size: int) -> str:
    """Format a byte count with a human-readable unit."""
    value = max(0.0, float(size))
    for unit in ('B', 'KB', 'MB', 'GB', 'TB'):
        if value < 1024 or unit == 'TB':
            return f'{value:.0f} {unit}' if unit == 'B' else f'{value:.1f} {unit}'
        value /= 1024
    return f'{value:.1f} TB'


def _truncate(value: Any, limit: int) -> str:
    """Coerce a value to text and keep it within Discord's field limits."""
    text = str(value)
    if len(text) <= limit:
        return text
    return text[:max(0, limit - 1)] + '…'


def _redact_urls(value: Any) -> str:
    """Hide signed download URLs before sending an error to Discord."""
    return _URL_RE.sub('<url>', str(value))


def _code(value: Any, limit: int = 1000) -> str:
    """Format a value as inline code without allowing it to break the field."""
    text = _truncate(value, limit - 2).replace('`', '\u02cb')
    return f'`{text}`'


def _retry_after(response) -> float:
    """Read Discord's ``retry_after`` hint from a 429 response."""
    try:
        value = float(response.json().get('retry_after'))
        return max(0.0, value)
    except (ValueError, TypeError, AttributeError):
        pass
    try:
        return max(0.0, float(response.headers.get('Retry-After', '1')))
    except (TypeError, ValueError):
        return 1.0


def _response_snippet(response) -> str:
    try:
        return _truncate(response.text, 200)
    except Exception:
        return ''


class DiscordNotifier:
    """Best-effort Discord notifications for a download session.

    A disabled notifier (no webhook URL) is a cheap no-op, so callers do not
    need to branch on whether notifications are configured.
    """

    def __init__(self, webhook_url: Optional[str] = None,
                 min_interval: float = MIN_SEND_INTERVAL,
                 timeout: float = REQUEST_TIMEOUT,
                 session: Optional[requests.Session] = None):
        self.webhook_url = (webhook_url or '').strip()
        self.min_interval = max(0.0, float(min_interval))
        self.timeout = timeout
        self.enabled = bool(self.webhook_url)
        self._queue = queue.Queue(maxsize=MAX_QUEUE_SIZE)
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._closed = False
        self._next_send_at = 0.0
        self._session = session
        self._owns_session = False

        if self.enabled and not self.webhook_url.lower().startswith(('http://', 'https://')):
            logger.warning('DISCORD_WEBHOOK_URL does not look like a URL; notifications disabled')
            self.enabled = False

        if self.enabled:
            if self._session is None:
                self._session = requests.Session()
                self._owns_session = True
            self._thread = threading.Thread(
                target=self._worker, name='discord-notifier', daemon=True,
            )
            self._thread.start()

    @classmethod
    def from_env(cls, env: Optional[Mapping[str, str]] = None) -> 'DiscordNotifier':
        """Build a notifier from ``DISCORD_WEBHOOK_URL``."""
        source = os.environ if env is None else env
        return cls(source.get(WEBHOOK_ENV, ''))

    def __enter__(self) -> 'DiscordNotifier':
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()

    def close(self, timeout: float = 5.0) -> None:
        """Flush queued notifications, then stop the worker thread."""
        self._closed = True
        if self._thread is not None:
            try:
                self._queue.put(None, timeout=1.0)
            except queue.Full:
                self._stop_event.set()
            self._thread.join(timeout)
            if self._thread.is_alive():
                logger.warning('Discord notifier did not finish within %.1fs', timeout)
            self._thread = None
        if self._owns_session and self._session is not None:
            self._session.close()

    # ------------------------------------------------------------------
    # Events
    # ------------------------------------------------------------------

    def session_started(self, total: int, skipped: int, parallel: int, output_dir: str) -> None:
        parts = f'{total} archive' + ('' if total == 1 else 's')
        description = f'Downloading **{parts}**'
        if skipped:
            description += f' ({skipped} already on disk)'
        self._enqueue(self._embed(
            '📦 Takeout download started',
            description,
            COLOR_INFO,
            [
                self._field('Parts to download', str(total)),
                self._field('Parallel', str(parallel)),
                self._field('Output', _code(output_dir), inline=False),
            ],
        ))

    def archive_completed(self, filename: str, size: int, completed: int,
                          total: int, bytes_downloaded: int) -> None:
        progress = f'{completed}/{total} archives' if total else str(completed)
        self._enqueue(self._embed(
            '✅ Archive downloaded',
            _code(filename),
            COLOR_SUCCESS,
            [
                self._field('Size', format_bytes(size)),
                self._field('Progress', progress),
                self._field('Total downloaded', format_bytes(bytes_downloaded)),
            ],
        ))

    def archive_failed(self, filename: str, error: str) -> None:
        self._enqueue(self._embed(
            '❌ Archive failed',
            _code(filename),
            COLOR_ERROR,
            [self._field('Error', _truncate(_redact_urls(error), 1000), inline=False)],
        ))

    def archive_not_found(self, filename: str) -> None:
        self._enqueue(self._embed(
            '⊘ Archive not found',
            f'{_code(filename)} returned a 404. It may not be part of this export.',
            COLOR_WARNING,
        ))

    def auth_required(self) -> None:
        self._enqueue(self._embed(
            '🔐 Authentication expired',
            'Google sign-in has expired. Provide a fresh cURL command to resume downloads.',
            COLOR_WARNING,
        ))

    def stopped(self, saved_partials: int = 0) -> None:
        if saved_partials:
            description = (
                f'Download stopped. {saved_partials} partial '
                f'archive{"" if saved_partials == 1 else "s"} kept for resume.'
            )
        else:
            description = 'Download stopped.'
        self._enqueue(self._embed('⏹ Takeout download stopped', description, COLOR_WARNING))

    def session_finished(self, completed: int, skipped: int, not_found: int,
                         failed: int, bytes_downloaded: int) -> None:
        if failed:
            description = (
                f'Finished with {failed} failed archive'
                f'{"" if failed == 1 else "s"}.'
            )
        else:
            description = 'All available Takeout archives have been processed.'
        self._enqueue(self._embed(
            '🎉 Takeout download finished',
            description,
            COLOR_SUCCESS,
            [
                self._field('Downloaded', str(completed)),
                self._field('Skipped', str(skipped)),
                self._field('Not found', str(not_found)),
                self._field('Failed', str(failed)),
                self._field('Data downloaded', format_bytes(bytes_downloaded), inline=False),
            ],
        ))

    def error(self, message: str) -> None:
        self._enqueue(self._embed(
            '❌ Download error',
            _truncate(_redact_urls(message), 1500),
            COLOR_ERROR,
        ))

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _enqueue(self, embed: Dict[str, Any]) -> None:
        if not self.enabled or self._closed or self._stop_event.is_set():
            return
        payload = {'username': USERNAME, 'embeds': [embed]}
        try:
            self._queue.put_nowait(payload)
        except queue.Full:
            logger.warning('Discord notification queue is full; dropping event')

    def _embed(self, title: str, description: str, color: int,
               fields: Optional[list] = None) -> Dict[str, Any]:
        embed: Dict[str, Any] = {
            'title': title,
            'color': color,
            'timestamp': datetime.now(timezone.utc).isoformat(),
        }
        text = _truncate(description, 4000).strip()
        if text:
            embed['description'] = text
        if fields:
            embed['fields'] = fields
        return embed

    @staticmethod
    def _field(name: str, value: str, inline: bool = True) -> Dict[str, Any]:
        text = _truncate(value, 1024).strip()
        return {'name': name, 'value': text or '—', 'inline': inline}

    def _worker(self) -> None:
        while True:
            try:
                payload = self._queue.get(timeout=0.5)
            except queue.Empty:
                if self._stop_event.is_set():
                    return
                continue
            if payload is None:
                return
            self._throttle()
            try:
                self._send(payload)
            except Exception:
                # A notification problem must never kill the worker (or the run).
                logger.exception('Unexpected error while sending Discord notification')

    def _throttle(self) -> None:
        delay = self._next_send_at - time.monotonic()
        if delay > 0:
            time.sleep(delay)
        self._next_send_at = time.monotonic() + self.min_interval

    def _send(self, payload: Dict[str, Any]) -> None:
        for attempt in range(MAX_RETRIES + 1):
            try:
                response = self._session.post(
                    self.webhook_url, json=payload, timeout=self.timeout,
                )
            except requests.RequestException as exc:
                logger.warning('Discord notification failed: %s', exc)
                if attempt >= MAX_RETRIES:
                    return
                self._stop_event.wait(min(2 ** (attempt + 1), 8))
                continue

            if response.status_code in (200, 204):
                return

            if response.status_code == 429:
                if attempt >= MAX_RETRIES:
                    logger.warning('Discord notification still rate limited; dropping event')
                    return
                wait = min(_retry_after(response), 30.0)
                logger.warning('Discord rate limited notification; retrying in %.1fs', wait)
                time.sleep(wait)
                continue

            if 500 <= response.status_code < 600:
                if attempt >= MAX_RETRIES:
                    logger.warning(
                        'Discord webhook unavailable (HTTP %s); dropping event',
                        response.status_code,
                    )
                    return
                self._stop_event.wait(min(2 ** (attempt + 1), 8))
                continue

            logger.warning(
                'Discord webhook rejected notification (HTTP %s): %s',
                response.status_code, _response_snippet(response),
            )
            if response.status_code in (401, 403, 404):
                # Invalid/deleted webhook: stop trying for the rest of the run.
                self._stop_event.set()
            return
