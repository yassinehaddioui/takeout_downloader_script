"""Tests for the optional Discord webhook notifier."""

import pytest

from discord_notifier import DiscordNotifier, format_bytes

WEBHOOK = 'https://discord.com/api/webhooks/123456789/abcdef'


class FakeResponse:
    def __init__(self, status_code=204, payload=None, text=''):
        self.status_code = status_code
        self._payload = payload or {}
        self.text = text
        self.headers = {}

    def json(self):
        return self._payload


class FakeSession:
    """Record webhook posts without touching the network."""

    def __init__(self, responses=None):
        self.responses = list(responses or [])
        self.calls = []
        self.closed = False

    def post(self, url, json=None, timeout=None):
        self.calls.append({'url': url, 'payload': json, 'timeout': timeout})
        if self.responses:
            return self.responses.pop(0)
        return FakeResponse()

    def close(self):
        self.closed = True


def test_notifier_is_disabled_without_webhook():
    notifier = DiscordNotifier.from_env(env={})

    assert notifier.enabled is False
    # Every event is a safe no-op.
    notifier.session_started(2, 1, 1, '/downloads')
    notifier.archive_completed('a.zip', 10, 1, 2, 10)
    notifier.close()


def test_archive_completed_sends_embed():
    session = FakeSession()
    notifier = DiscordNotifier(WEBHOOK, min_interval=0, session=session)

    notifier.archive_completed(
        filename='takeout-20251207T071725Z-3-001.zip',
        size=2048,
        completed=2,
        total=5,
        bytes_downloaded=8192,
    )
    notifier.close()

    assert len(session.calls) == 1
    call = session.calls[0]
    assert call['url'] == WEBHOOK
    payload = call['payload']
    embed = payload['embeds'][0]
    assert embed['title'] == '✅ Archive downloaded'
    assert 'takeout-20251207T071725Z-3-001.zip' in embed['description']
    fields = {field['name']: field['value'] for field in embed['fields']}
    assert fields == {
        'Size': '2.0 KB',
        'Progress': '2/5 archives',
        'Total downloaded': '8.0 KB',
    }


def test_all_events_produce_embeds():
    session = FakeSession()
    notifier = DiscordNotifier(WEBHOOK, min_interval=0, session=session)

    notifier.session_started(3, 1, 2, '/downloads')
    notifier.archive_failed('bad.zip', 'network error')
    notifier.archive_not_found('missing.zip')
    notifier.auth_required()
    notifier.stopped(2)
    notifier.session_finished(1, 1, 0, 1, 4096)
    notifier.error('unexpected')
    notifier.close()

    assert len(session.calls) == 7
    titles = [call['payload']['embeds'][0]['title'] for call in session.calls]
    assert titles == [
        '📦 Takeout download started',
        '❌ Archive failed',
        '⊘ Archive not found',
        '🔐 Authentication expired',
        '⏹ Takeout download stopped',
        '🎉 Takeout download finished',
        '❌ Download error',
    ]


def test_error_redacts_signed_urls():
    session = FakeSession()
    notifier = DiscordNotifier(WEBHOOK, min_interval=0, session=session)

    notifier.error(
        "network error for 'https://takeout-download.usercontent.google.com/"
        "download/takeout-20251207-3-001.zip?i=0&token=secret'"
    )
    notifier.close()

    embed = session.calls[0]['payload']['embeds'][0]
    assert 'secret' not in embed['description']
    assert '<url>' in embed['description']


def test_rate_limited_event_is_retried():
    session = FakeSession([
        FakeResponse(429, {'retry_after': 0}),
        FakeResponse(204),
    ])
    notifier = DiscordNotifier(WEBHOOK, min_interval=0, session=session)

    notifier.error('boom')
    notifier.close()

    assert len(session.calls) == 2
    assert session.calls[0]['payload'] == session.calls[1]['payload']


def test_rejected_webhook_disables_further_events():
    session = FakeSession([FakeResponse(404, text='Unknown Webhook')])
    notifier = DiscordNotifier(WEBHOOK, min_interval=0, session=session)

    notifier.error('first')
    notifier.close()

    assert len(session.calls) == 1
    # Closing an injected session must not close the caller's session.
    assert session.closed is False


def test_events_are_dropped_after_close():
    session = FakeSession()
    notifier = DiscordNotifier(WEBHOOK, min_interval=0, session=session)
    notifier.close()

    notifier.error('too late')
    notifier.close()

    assert session.calls == []


def test_from_env_reads_webhook():
    notifier = DiscordNotifier.from_env(env={'DISCORD_WEBHOOK_URL': WEBHOOK})
    try:
        assert notifier.enabled is True
        assert notifier.webhook_url == WEBHOOK
    finally:
        notifier.close()


def test_invalid_webhook_url_is_disabled():
    notifier = DiscordNotifier('not-a-url')
    try:
        assert notifier.enabled is False
    finally:
        notifier.close()


@pytest.mark.parametrize('size, expected', [
    (0, '0 B'),
    (1023, '1023 B'),
    (1024, '1.0 KB'),
    (1024 * 1024, '1.0 MB'),
    (1536 * 1024 * 1024, '1.5 GB'),
])
def test_format_bytes(size, expected):
    assert format_bytes(size) == expected
