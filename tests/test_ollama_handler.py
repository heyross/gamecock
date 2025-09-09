from contextlib import contextmanager
import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import gamecock.ollama_handler as oh
from gamecock.ollama_handler import OllamaHandler


def test_default_config_fallback(monkeypatch):
    # Force psutil to raise to exercise fallback defaults
    monkeypatch.setattr(oh, "psutil", SimpleNamespace(
        cpu_count=lambda logical=False: (_ for _ in ()).throw(RuntimeError("cpu error")),
        virtual_memory=lambda: (_ for _ in ()).throw(RuntimeError("mem error")),
    ))

    h = OllamaHandler()
    cfg = h.get_config()
    assert cfg["parameters"]["num_ctx"] == 4096
    assert cfg["parameters"]["num_thread"] == 2


def test_is_running_true_false(monkeypatch):
    h = OllamaHandler()

    class Resp:
        def __init__(self, code):
            self.status_code = code

    # True
    monkeypatch.setattr(oh.httpx, "get", lambda *a, **k: Resp(200))
    assert h.is_running() is True

    # False
    monkeypatch.setattr(oh.httpx, "get", lambda *a, **k: Resp(500))
    assert h.is_running() is False

    # Exception -> False
    def boom(*a, **k):
        raise RuntimeError("boom")
    monkeypatch.setattr(oh.httpx, "get", boom)
    assert h.is_running() is False


def test_is_model_available(monkeypatch):
    h = OllamaHandler(model="mymodel")

    class Resp:
        def __init__(self, code, payload=None):
            self.status_code = code
            self._payload = payload or {}
        def json(self):
            return self._payload

    # Not 200
    monkeypatch.setattr(oh.httpx, "get", lambda *a, **k: Resp(500))
    assert h.is_model_available() is False

    # 200 but not found
    monkeypatch.setattr(oh.httpx, "get", lambda *a, **k: Resp(200, {"models": [{"name": "other"}]}))
    assert h.is_model_available() is False

    # 200 and found
    monkeypatch.setattr(oh.httpx, "get", lambda *a, **k: Resp(200, {"models": [{"name": "mymodel"}]}))
    assert h.is_model_available() is True

    # Exception
    def boom(*a, **k):
        raise RuntimeError("boom")
    monkeypatch.setattr(oh.httpx, "get", boom)
    assert h.is_model_available() is False


def test_generate_success_and_variants(monkeypatch):
    h = OllamaHandler(model="mymodel")

    class Resp:
        def __init__(self, code, payload=None):
            self.status_code = code
            self._payload = payload or {}
        def json(self):
            return self._payload

    captured = {}
    def fake_post(url, json=None, timeout=None):
        captured["json"] = json
        return Resp(200, {"response": "hello"})

    monkeypatch.setattr(oh.httpx, "post", fake_post)

    # Success without max_tokens
    msg = h.generate("prompt")
    assert msg == "hello"
    assert "num_predict" not in captured["json"]["options"]

    # Success with max_tokens -> ensure included
    msg = h.generate("prompt", max_tokens=128)
    assert captured["json"]["options"]["num_predict"] == 128

    # Non-200 -> None
    def fake_post_non200(url, json=None, timeout=None):
        return Resp(500, {})
    monkeypatch.setattr(oh.httpx, "post", fake_post_non200)
    assert h.generate("prompt") is None

    # Exception -> None
    def boom(*a, **k):
        raise RuntimeError("boom")
    monkeypatch.setattr(oh.httpx, "post", boom)
    assert h.generate("prompt") is None


def test_list_models(monkeypatch):
    h = OllamaHandler()

    class Resp:
        def __init__(self, code, payload=None):
            self.status_code = code
            self._payload = payload or {}
        def json(self):
            return self._payload

    monkeypatch.setattr(oh.httpx, "get", lambda *a, **k: Resp(200, {"models": [{"name": "a"}, {"name": "b"}]}))
    assert h.list_models() == ["a", "b"]

    monkeypatch.setattr(oh.httpx, "get", lambda *a, **k: Resp(500))
    assert h.list_models() == []

    def boom(*a, **k):
        raise RuntimeError("boom")
    monkeypatch.setattr(oh.httpx, "get", boom)
    assert h.list_models() == []


def test_pull_model_happy_and_error(monkeypatch):
    h = OllamaHandler(model="mymodel")

    # Fake streaming response
    class FakeStreamResp:
        def __init__(self, status_code, lines):
            self.status_code = status_code
            self._lines = lines
        def __enter__(self):
            return self
        def __exit__(self, exc_type, exc, tb):
            return False
        def iter_lines(self):
            for line in self._lines:
                yield json.dumps(line)

    def fake_stream(method, url, json=None, timeout=None):
        # First a happy path with progress updates
        return FakeStreamResp(200, [
            {"status": "starting"},
            {"total": 100, "completed": 10, "status": "downloading"},
            {"total": 100, "completed": 100, "status": "done"},
        ])

    monkeypatch.setattr(oh.httpx, "stream", fake_stream)
    # Should not raise
    h.pull_model()

    # Non-200 status
    def fake_stream_bad(method, url, json=None, timeout=None):
        return FakeStreamResp(500, [])
    monkeypatch.setattr(oh.httpx, "stream", fake_stream_bad)
    h.pull_model()

    # Exception path
    def boom(*a, **k):
        raise RuntimeError("boom")
    monkeypatch.setattr(oh.httpx, "stream", boom)
    h.pull_model()
