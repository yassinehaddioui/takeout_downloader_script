"""Shared pytest fixtures."""

import pytest


@pytest.fixture(autouse=True)
def disable_discord_notifications(monkeypatch):
    """Never send real Discord messages from the test suite."""
    monkeypatch.delenv('DISCORD_WEBHOOK_URL', raising=False)
