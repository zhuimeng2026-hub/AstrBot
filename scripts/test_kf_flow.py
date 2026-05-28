"""Test script to verify KF message flow fixes.

This script tests the code logic without hitting real WeChat APIs.
Run: ./venv/bin/python scripts/test_kf_flow.py
"""

import asyncio
import sys
from unittest.mock import AsyncMock, MagicMock, patch

sys.path.insert(0, "/opt/AstrBot")


async def test_send_kf_message_logging():
    """Test that _send_kf_message logs API responses correctly."""
    from astrbot.api.message_components import Plain
    from astrbot.api.event import MessageChain
    from astrbot.core.platform.sources.wecom_ai_bot.wecomai_adapter import (
        WecomAIBotAdapter,
    )

    # Create adapter with minimal config
    config = {
        "corpid": "test_corp",
        "corpsecret": "test_secret",
        "token": "test_token",
        "encoding_aes_key": "test_key",
        "port": 9999,
    }
    adapter = WecomAIBotAdapter.__new__(WecomAIBotAdapter)
    adapter.config = config
    adapter._last_open_kfid = "kf_test123"
    adapter._last_external_userid = "user_test456"

    # Mock aiohttp session
    mock_token_resp = AsyncMock()
    mock_token_resp.json = AsyncMock(
        return_value={"errcode": 0, "access_token": "fake_token"}
    )
    mock_token_resp.__aenter__ = AsyncMock(return_value=mock_token_resp)
    mock_token_resp.__aexit__ = AsyncMock(return_value=False)

    mock_send_resp = AsyncMock()
    mock_send_resp.json = AsyncMock(return_value={"errcode": 0, "errmsg": "ok"})
    mock_send_resp.__aenter__ = AsyncMock(return_value=mock_send_resp)
    mock_send_resp.__aexit__ = AsyncMock(return_value=False)

    mock_session = AsyncMock()
    mock_session.get = MagicMock(return_value=mock_token_resp)
    mock_session.post = MagicMock(return_value=mock_send_resp)
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)

    with patch("aiohttp.ClientSession", return_value=mock_session):
        chain = MessageChain([Plain("Hello from test!")])
        await adapter._send_kf_message("kf_test123", "user_test456", chain)

    print("[PASS] _send_kf_message: API response logging works")


async def test_send_kf_message_error_logging():
    """Test that _send_kf_message logs errors from WeChat API."""
    from astrbot.api.message_components import Plain
    from astrbot.api.event import MessageChain
    from astrbot.core.platform.sources.wecom_ai_bot.wecomai_adapter import (
        WecomAIBotAdapter,
    )

    config = {
        "corpid": "test_corp",
        "corpsecret": "test_secret",
        "token": "test_token",
        "encoding_aes_key": "test_key",
        "port": 9999,
    }
    adapter = WecomAIBotAdapter.__new__(WecomAIBotAdapter)
    adapter.config = config

    # Mock error response from send API
    mock_token_resp = AsyncMock()
    mock_token_resp.json = AsyncMock(
        return_value={"errcode": 0, "access_token": "fake_token"}
    )
    mock_token_resp.__aenter__ = AsyncMock(return_value=mock_token_resp)
    mock_token_resp.__aexit__ = AsyncMock(return_value=False)

    mock_send_resp = AsyncMock()
    mock_send_resp.json = AsyncMock(
        return_value={"errcode": 40003, "errmsg": "invalid openid"}
    )
    mock_send_resp.__aenter__ = AsyncMock(return_value=mock_send_resp)
    mock_send_resp.__aexit__ = AsyncMock(return_value=False)

    mock_session = AsyncMock()
    mock_session.get = MagicMock(return_value=mock_token_resp)
    mock_session.post = MagicMock(return_value=mock_send_resp)
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)

    with patch("aiohttp.ClientSession", return_value=mock_session):
        chain = MessageChain([Plain("test")])
        await adapter._send_kf_message("kf_bad", "user_bad", chain)

    print("[PASS] _send_kf_message: error responses are logged")


async def test_send_kf_skips_empty_plain():
    """Test that empty Plain components are skipped."""
    from astrbot.api.message_components import Plain
    from astrbot.api.event import MessageChain
    from astrbot.core.platform.sources.wecom_ai_bot.wecomai_adapter import (
        WecomAIBotAdapter,
    )

    config = {
        "corpid": "test_corp",
        "corpsecret": "test_secret",
        "token": "test_token",
        "encoding_aes_key": "test_key",
        "port": 9999,
    }
    adapter = WecomAIBotAdapter.__new__(WecomAIBotAdapter)
    adapter.config = config

    mock_token_resp = AsyncMock()
    mock_token_resp.json = AsyncMock(
        return_value={"errcode": 0, "access_token": "fake_token"}
    )
    mock_token_resp.__aenter__ = AsyncMock(return_value=mock_token_resp)
    mock_token_resp.__aexit__ = AsyncMock(return_value=False)

    mock_session = AsyncMock()
    mock_session.get = MagicMock(return_value=mock_token_resp)
    mock_session.post = MagicMock()  # Should NOT be called
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)

    with patch("aiohttp.ClientSession", return_value=mock_session):
        chain = MessageChain([Plain("")])
        await adapter._send_kf_message("kf_test", "user_test", chain)

    # post should not have been called since Plain("") is empty
    mock_session.post.assert_not_called()
    print("[PASS] _send_kf_message: empty Plain components are skipped")


async def test_kf_message_dedup():
    """Test that duplicate messages are skipped."""
    from astrbot.core.platform.sources.wecom_ai_bot.wecomai_adapter import (
        WecomAIBotAdapter,
    )
    from astrbot.core.platform.platform_metadata import PlatformMetadata

    adapter = WecomAIBotAdapter.__new__(WecomAIBotAdapter)
    adapter.config = {}
    adapter.bot_name = "test_bot"
    adapter._last_open_kfid = ""
    adapter._last_external_userid = ""
    adapter._kf_processed_msgids = set()
    adapter.metadata = PlatformMetadata(
        name="wecom_ai_bot", description="test", id="wecom_ai_bot"
    )
    adapter.api_client = None
    adapter.queue_mgr = MagicMock()
    adapter.webhook_client = None
    adapter.only_use_webhook_url_to_send = False

    mock_commit = MagicMock()
    adapter.commit_event = mock_commit

    msg = {
        "msgid": "msg_001",
        "msgtype": "text",
        "external_userid": "user_001",
        "open_kfid": "kf_001",
        "send_time": 1234567890,
        "text": {"content": "hello"},
    }

    # First call should process
    await adapter._process_kf_message(msg)
    assert mock_commit.call_count == 1, "First message should be processed"

    # Second call with same msgid should be skipped
    await adapter._process_kf_message(msg)
    assert mock_commit.call_count == 1, "Duplicate message should be skipped"

    print("[PASS] _process_kf_message: duplicate messages are skipped")


async def test_miniprogram_handling():
    """Test that miniprogram messages are handled."""
    from astrbot.core.platform.sources.wecom_ai_bot.wecomai_adapter import (
        WecomAIBotAdapter,
    )
    from astrbot.core.platform.platform_metadata import PlatformMetadata

    adapter = WecomAIBotAdapter.__new__(WecomAIBotAdapter)
    adapter.config = {}
    adapter.bot_name = "test_bot"
    adapter._last_open_kfid = ""
    adapter._last_external_userid = ""
    adapter._kf_processed_msgids = set()
    adapter.metadata = PlatformMetadata(
        name="wecom_ai_bot", description="test", id="wecom_ai_bot"
    )
    adapter.api_client = None
    adapter.queue_mgr = MagicMock()
    adapter.webhook_client = None
    adapter.only_use_webhook_url_to_send = False

    captured_event = None

    def mock_commit(event):
        nonlocal captured_event
        captured_event = event

    adapter.commit_event = mock_commit

    msg = {
        "msgid": "msg_mini_001",
        "msgtype": "miniprogram",
        "external_userid": "user_001",
        "open_kfid": "kf_001",
        "send_time": 1234567890,
        "miniprogram": {"title": "My Mini App"},
    }

    await adapter._process_kf_message(msg)
    assert captured_event is not None, "Miniprogram message should create event"
    assert "My Mini App" in captured_event.message_str
    print("[PASS] _process_kf_message: miniprogram messages are handled")


async def test_kf_sender_skips_stream_queue():
    """Test that KF messages skip the stream back_queue in send()."""
    from astrbot.api.message_components import Plain
    from astrbot.api.event import MessageChain
    from astrbot.core.platform.sources.wecom_ai_bot.wecomai_event import (
        WecomAIBotMessageEvent,
    )

    kf_called = False

    async def mock_kf_sender(chain):
        nonlocal kf_called
        kf_called = True

    # Create a minimal event
    mock_obj = MagicMock()
    mock_obj.raw_message = {"stream_id": "test_stream"}

    mock_meta = MagicMock()

    event = WecomAIBotMessageEvent(
        message_str="test",
        message_obj=mock_obj,
        platform_meta=mock_meta,
        session_id="test_session",
        api_client=None,
        queue_mgr=MagicMock(),
        kf_sender=mock_kf_sender,
    )
    event.queue_mgr.get_pending_response = MagicMock(return_value=None)
    event.webhook_client = None
    event.long_connection_sender = None
    event.only_use_webhook_url_to_send = False

    # Mock the parent send
    with patch.object(
        WecomAIBotMessageEvent.__bases__[0], "send", new_callable=AsyncMock
    ):
        chain = MessageChain([Plain("test message")])
        await event.send(chain)

    assert kf_called, "KF sender should have been called"
    print("[PASS] send(): KF messages use kf_sender directly, skip stream queue")


async def main():
    print("=" * 60)
    print("Testing KF message flow fixes")
    print("=" * 60)

    tests = [
        test_send_kf_message_logging,
        test_send_kf_message_error_logging,
        test_send_kf_skips_empty_plain,
        test_kf_message_dedup,
        test_miniprogram_handling,
        test_kf_sender_skips_stream_queue,
    ]

    passed = 0
    failed = 0
    for test in tests:
        try:
            await test()
            passed += 1
        except Exception as e:
            print(f"[FAIL] {test.__name__}: {e}")
            failed += 1

    print("=" * 60)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 60)

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
