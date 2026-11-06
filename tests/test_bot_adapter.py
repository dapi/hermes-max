"""Bot-mode adapter tests against a mocked MAX Bot API (no network).

Needs the Hermes Agent sources importable, e.g.::

    PYTHONPATH=~/.hermes/hermes-agent python -m pytest tests/

or run this file directly with the Hermes runtime python (``main()`` below).
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import sys
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parent.parent
PKG = "hermes_max_plugin"


def _load_plugin():
    if PKG in sys.modules:
        return sys.modules[PKG]
    spec = importlib.util.spec_from_file_location(
        PKG, ROOT / "__init__.py", submodule_search_locations=[str(ROOT)]
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[PKG] = module
    spec.loader.exec_module(module)
    return module


_load_plugin()
adapter_mod = importlib.import_module(f"{PKG}.adapter")
bot = importlib.import_module(f"{PKG}.bot_adapter")

from gateway.config import PlatformConfig  # noqa: E402
from gateway.platform_registry import PlatformEntry, platform_registry  # noqa: E402
from gateway.platforms.base import MessageType  # noqa: E402


class _Ctx:
    def register_platform(self, **kwargs):
        platform_registry.register(
            PlatformEntry(source="plugin", plugin_name="max-platform", **kwargs)
        )


adapter_mod.register(_Ctx())

ME = {"user_id": 100, "name": "Koko", "username": "koko_bot", "is_bot": True}
SENDER = {"user_id": 7, "first_name": "Danil", "name": "Danil", "is_bot": False}


def make_adapter(handler, monkeypatch_env=None, extra=None):
    """Adapter wired to a MockTransport; inbound events are collected in ``.events``."""
    import os

    os.environ["MAX_BOT_TOKEN"] = "test-token"
    ad = bot.MaxBotAdapter(PlatformConfig(enabled=True, extra=extra or {}))
    ad._client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url=bot.DEFAULT_API_BASE,
        headers={"Authorization": "test-token"},
    )
    ad._me_id = ME["user_id"]
    ad.events = []

    async def collect(event):
        ad.events.append(event)

    ad.handle_message = collect
    ad.set_message_handler(lambda event: None)
    return ad


def message_update(text="hi", *, mid="mid.1", attachments=None, link=None, chat_type="dialog", sender=None):
    message = {
        "sender": sender or SENDER,
        "recipient": {"chat_id": 555, "chat_type": chat_type, "user_id": ME["user_id"]},
        "timestamp": 1_793_900_000_000,
        "body": {"mid": mid, "seq": 1, "text": text, "attachments": attachments or []},
    }
    if link:
        message["link"] = link
    return {"update_type": "message_created", "timestamp": 1_793_900_000_000, "message": message}


def run(coro):
    return asyncio.run(coro)


def _no_http(request):  # inbound-only tests must not call the API
    raise AssertionError(f"unexpected request {request.method} {request.url}")


# ── mode resolution ────────────────────────────────────────────────────────


def test_bot_token_selects_bot_mode():
    import os

    os.environ["MAX_BOT_TOKEN"] = "test-token"
    os.environ.pop("MAX_MODE", None)
    assert adapter_mod._resolve_mode({}) == "bot"
    assert adapter_mod._resolve_mode({"mode": "user"}) == "user"
    assert adapter_mod.check_requirements() is True
    cfg = PlatformConfig(enabled=True, extra={})
    assert adapter_mod.validate_config(cfg) is True
    assert isinstance(adapter_mod._create_adapter(cfg), bot.MaxBotAdapter)


# ── inbound ────────────────────────────────────────────────────────────────


def test_text_message_becomes_event():
    ad = make_adapter(_no_http)
    run(ad._handle_update(message_update("привет")))
    (event,) = ad.events
    assert event.text == "привет"
    assert event.message_type == MessageType.TEXT
    assert event.source.chat_id == "555"
    assert event.source.chat_type == "dm"
    assert event.source.user_id == "7"
    assert event.user_name == "Danil"
    assert event.message_id == "mid.1"


def test_duplicates_own_messages_and_empty_are_dropped():
    ad = make_adapter(_no_http)
    run(ad._handle_update(message_update("one", mid="mid.a")))
    run(ad._handle_update(message_update("one", mid="mid.a")))  # replayed
    run(ad._handle_update(message_update("me", mid="mid.b", sender=ME)))  # own echo
    run(ad._handle_update(message_update("", mid="mid.c")))  # nothing to say
    assert [e.text for e in ad.events] == ["one"]


def test_slash_command_and_group_chat():
    ad = make_adapter(_no_http)
    run(ad._handle_update(message_update("/new", chat_type="chat")))
    (event,) = ad.events
    assert event.message_type == MessageType.COMMAND
    assert event.source.chat_type == "group"


def test_reply_and_forward_links():
    ad = make_adapter(_no_http)
    reply = {"type": "reply", "sender": ME, "message": {"mid": "mid.old", "text": "earlier answer"}}
    run(ad._handle_update(message_update("and?", mid="mid.r", link=reply)))
    forward = {
        "type": "forward",
        "sender": {"user_id": 9, "name": "Ivan"},
        "message": {"mid": "mid.x", "text": "forwarded body"},
    }
    run(ad._handle_update(message_update("look", mid="mid.f", link=forward)))
    replied, forwarded = ad.events
    assert replied.reply_to_message_id == "mid.old"
    assert replied.reply_to_text == "earlier answer"
    assert replied.reply_to_is_own_message is True
    assert forwarded.text == "look\n[Forwarded from Ivan]\nforwarded body"


def test_attachments_are_cached_or_described():
    ad = make_adapter(_no_http)
    downloads = []

    async def fake_download(url, *, filename, default_kind, default_ext):
        downloads.append((url, default_kind))
        return f"/cache/{default_kind or 'doc'}", f"{default_kind or 'application'}/x"

    ad._download_to_cache = fake_download
    attachments = [
        {"type": "image", "payload": {"url": "https://cdn/img", "token": "t"}},
        {"type": "file", "filename": "a.pdf", "payload": {"url": "https://cdn/file"}},
        {"type": "location", "latitude": 55.7, "longitude": 37.6},
        {"type": "sticker", "payload": {"url": "https://cdn/s", "code": "x"}},
    ]
    run(ad._handle_update(message_update("", attachments=attachments)))
    (event,) = ad.events
    assert downloads == [("https://cdn/img", "image"), ("https://cdn/file", None)]
    assert event.media_urls == ["/cache/image", "/cache/doc"]
    assert event.message_type == MessageType.PHOTO
    assert event.text == "[location: 55.7, 37.6] [sticker]"


def test_voice_message_is_voice_type():
    ad = make_adapter(_no_http)

    async def fake_download(url, **kwargs):
        return "/cache/voice.ogg", "audio/ogg"

    ad._download_to_cache = fake_download
    run(ad._handle_update(message_update("", attachments=[{"type": "audio", "payload": {"url": "https://cdn/a"}}])))
    (event,) = ad.events
    assert event.message_type == MessageType.VOICE
    assert event.media_types == ["audio/ogg"]


def test_bot_started_is_start_command():
    ad = make_adapter(_no_http)
    update = {"update_type": "bot_started", "timestamp": 1, "chat_id": 555, "user": SENDER, "payload": "ref"}
    run(ad._handle_update(update))
    (event,) = ad.events
    assert event.text == "/start ref"
    assert event.message_type == MessageType.COMMAND
    assert event.source.user_id == "7"


def test_first_run_skips_backlog_only():
    ad = make_adapter(_no_http)
    ad._first_run = True
    old = message_update("old", mid="mid.old")
    old["timestamp"] = 1_000  # 1970 — far older than the adapter start
    run(ad._handle_update(old))
    assert ad.events == []
    ad._first_run = False
    run(ad._handle_update(old))
    assert [e.text for e in ad.events] == ["old"]


# ── polling ────────────────────────────────────────────────────────────────


def test_poll_loop_dispatches_and_persists_marker(tmp_path=None):
    import tempfile

    state = Path(tempfile.mkdtemp()) / "state.json"
    calls = []

    def handler(request):
        calls.append(dict(request.url.params))
        if len(calls) == 1:
            return httpx.Response(200, json={"updates": [message_update("first")], "marker": 42})
        ad._running = False  # stop after the second poll
        return httpx.Response(200, json={"updates": [], "marker": 42})

    ad = make_adapter(handler)
    ad._marker_path = lambda: state
    ad._running = True
    run(ad._poll_loop())
    assert [e.text for e in ad.events] == ["first"]
    assert "marker" not in calls[0] and calls[1]["marker"] == "42"
    assert calls[0]["types"] == bot.UPDATE_TYPES
    assert json.loads(state.read_text())["markers"] == {"100": 42}
    assert ad._load_marker() == 42


def test_poll_loop_stops_on_rejected_token():
    ad = make_adapter(lambda request: httpx.Response(401, json={"code": "verify.token", "message": "bad"}))
    ad._running = True
    run(ad._poll_loop())
    assert ad._running is False  # fatal, no retry storm


class _WebhookRequest:
    def __init__(self, payload, secret=None):
        self._payload = payload
        self.headers = {}
        if secret is not None:
            self.headers["X-Max-Bot-Api-Secret"] = secret

    async def json(self):
        return self._payload


def test_webhook_dispatches_voice_update():
    ad = make_adapter(_no_http, extra={"webhook_secret": "hook-secret"})

    async def fake_download(url, **kwargs):
        return "/cache/voice.ogg", "audio/ogg"

    ad._download_to_cache = fake_download
    update = message_update(
        "",
        attachments=[{"type": "audio", "payload": {"url": "https://cdn/voice", "token": "t"}}],
    )
    response = run(ad._handle_webhook(_WebhookRequest(update, "hook-secret")))
    assert response.status == 200
    (event,) = ad.events
    assert event.message_type == MessageType.VOICE
    assert event.media_urls == ["/cache/voice.ogg"]


def test_webhook_rejects_wrong_secret():
    ad = make_adapter(_no_http, extra={"webhook_secret": "hook-secret"})
    response = run(ad._handle_webhook(_WebhookRequest(message_update("no"), "wrong")))
    assert response.status == 401
    assert ad.events == []


def test_webhook_subscription_is_registered_once():
    calls = []

    def handler(request):
        calls.append((request.method, request.url.path, json.loads(request.content) if request.content else None))
        if request.method == "GET":
            return httpx.Response(200, json={"subscriptions": []})
        return httpx.Response(200, json={"success": True})

    ad = make_adapter(
        handler,
        extra={
            "webhook_url": "https://max-hooks.office.brandymint.ru/max/webhook",
            "webhook_secret": "hook-secret",
        },
    )
    run(ad._ensure_webhook_subscription())
    assert calls == [
        ("GET", "/subscriptions", None),
        (
            "POST",
            "/subscriptions",
            {
                "url": "https://max-hooks.office.brandymint.ru/max/webhook",
                "update_types": ["message_created", "bot_started"],
                "secret": "hook-secret",
            },
        ),
    ]


def test_existing_webhook_subscription_is_not_duplicated():
    calls = []

    def handler(request):
        calls.append((request.method, request.url.path))
        return httpx.Response(
            200,
            json={
                "subscriptions": [
                    {
                        "url": "https://max-hooks.office.brandymint.ru/max/webhook",
                        "time": 1,
                        "update_types": ["message_created", "bot_started"],
                    }
                ]
            },
        )

    ad = make_adapter(
        handler,
        extra={
            "webhook_url": "https://max-hooks.office.brandymint.ru/max/webhook",
            "webhook_secret": "hook-secret",
        },
    )
    run(ad._ensure_webhook_subscription())
    assert calls == [("GET", "/subscriptions")]


# ── outbound ───────────────────────────────────────────────────────────────


def _recording_handler(responses):
    sent = []

    def handler(request):
        body = json.loads(request.content) if request.content else None
        sent.append((request.method, request.url.path, dict(request.url.params), body))
        return responses(len(sent), request)

    return sent, handler


def test_send_uses_markdown_reply_and_auth_header():
    seen_auth = []

    def responses(n, request):
        seen_auth.append(request.headers.get("authorization"))
        return httpx.Response(200, json={"message": {"body": {"mid": f"mid.out{n}"}}})

    sent, handler = _recording_handler(responses)
    ad = make_adapter(handler)
    result = run(ad.send("555", "**hello**", reply_to="mid.1"))
    assert result.success and result.message_id == "mid.out1"
    method, path, params, body = sent[0]
    assert (method, path, params) == ("POST", "/messages", {"chat_id": "555"})
    assert body == {"text": "**hello**", "format": "markdown", "link": {"type": "reply", "mid": "mid.1"}}
    assert seen_auth == ["test-token"]


def test_long_message_is_split_and_last_id_returned():
    sent, handler = _recording_handler(
        lambda n, request: httpx.Response(200, json={"message": {"body": {"mid": f"mid.out{n}"}}})
    )
    ad = make_adapter(handler)
    result = run(ad.send("555", ("word " * 1500).strip()))
    assert result.success and len(sent) >= 2
    assert all(len(body["text"]) <= bot.MAX_TEXT_LIMIT for _, _, _, body in sent)
    assert result.message_id == f"mid.out{len(sent)}"
    assert len(result.continuation_message_ids) == len(sent) - 1


def test_rejected_markdown_falls_back_to_plain_text():
    def responses(n, request):
        if n == 1:
            return httpx.Response(400, json={"code": "proto.payload", "message": "bad markup"})
        return httpx.Response(200, json={"message": {"body": {"mid": "mid.plain"}}})

    sent, handler = _recording_handler(responses)
    ad = make_adapter(handler)
    result = run(ad.send("555", "a * b"))
    assert result.success and result.message_id == "mid.plain"
    assert "format" in sent[0][3] and "format" not in sent[1][3]


def test_send_failure_is_reported_not_raised():
    ad = make_adapter(lambda request: httpx.Response(403, json={"code": "chat.denied", "message": "no"}))
    result = run(ad.send("555", "hi"))
    assert result.success is False and "chat.denied" in result.error
    assert result.retryable is False


def test_document_upload_and_attachment_retry(tmp_path=None):
    import tempfile

    bot.ATTACHMENT_RETRY_DELAYS = (0.0, 0.0)
    path = Path(tempfile.mkdtemp()) / "report.pdf"
    path.write_bytes(b"%PDF-test")
    log = []

    def handler(request):
        log.append((request.method, str(request.url)))
        if request.url.path == "/uploads":
            assert request.url.params["type"] == "file"
            return httpx.Response(200, json={"url": "https://upload.example/put?id=1"})
        if request.url.host == "upload.example":
            assert b"report.pdf" in request.content
            return httpx.Response(200, json={"token": "file-token"})
        posts = [entry for entry in log if entry[1].startswith(bot.DEFAULT_API_BASE + "/messages")]
        if len(posts) == 1:
            return httpx.Response(400, json={"code": "attachment.not.ready", "message": "wait"})
        body = json.loads(request.content)
        assert body["attachments"] == [{"type": "file", "payload": {"token": "file-token"}}]
        assert body["text"] == "отчёт"
        return httpx.Response(200, json={"message": {"body": {"mid": "mid.doc"}}})

    ad = make_adapter(handler)
    result = run(ad.send_document("555", str(path), caption="отчёт"))
    assert result.success and result.message_id == "mid.doc"


def test_video_upload_uses_slot_token():
    import tempfile

    path = Path(tempfile.mkdtemp()) / "clip.mp4"
    path.write_bytes(b"\x00\x00")
    bodies = []

    def handler(request):
        if request.url.path == "/uploads":
            return httpx.Response(200, json={"url": "https://upload.example/v", "token": "slot-token"})
        if request.url.host == "upload.example":
            return httpx.Response(200, text="<retval>1</retval>")
        bodies.append(json.loads(request.content))
        return httpx.Response(200, json={"message": {"body": {"mid": "mid.v"}}})

    ad = make_adapter(handler)
    result = run(ad.send_video("555", str(path)))
    assert result.success
    assert bodies[0]["attachments"] == [{"type": "video", "payload": {"token": "slot-token"}}]


def test_edit_and_typing():
    sent, handler = _recording_handler(lambda n, request: httpx.Response(200, json={"success": True}))
    ad = make_adapter(handler)
    assert run(ad.edit_message("555", "mid.1", "new text")).success
    run(ad.send_typing("555"))
    assert sent[0][:3] == ("PUT", "/messages", {"message_id": "mid.1"})
    assert sent[1][:2] == ("POST", "/chats/555/actions") and sent[1][3] == {"action": "typing_on"}


def main() -> int:
    failed = 0
    tests = [(name, fn) for name, fn in sorted(globals().items()) if name.startswith("test_") and callable(fn)]
    for name, fn in tests:
        try:
            fn()
            print(f"ok   {name}")
        except Exception as exc:  # noqa: BLE001
            failed += 1
            print(f"FAIL {name}: {type(exc).__name__}: {exc}")
    print(f"{len(tests) - failed}/{len(tests)} passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
