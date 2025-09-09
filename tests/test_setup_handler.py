import builtins
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from gamecock.setup_handler import SetupHandler
import gamecock.setup_handler as sh


@pytest.fixture()
def temp_data_dir(tmp_path: Path):
    # Provide an isolated data directory per test
    d = tmp_path / "data"
    d.mkdir()
    return d


def test_check_and_install_skips_when_flag_exists(temp_data_dir, monkeypatch):
    handler = SetupHandler(data_dir=temp_data_dir)
    # Create the flag to simulate already completed
    handler.setup_complete_flag.touch()

    mock_run = MagicMock()
    monkeypatch.setattr(subprocess, "run", mock_run)

    handler.check_and_install_prerequisites()

    mock_run.assert_not_called()


def test_check_and_install_success_creates_flag_and_installs(temp_data_dir, monkeypatch):
    handler = SetupHandler(data_dir=temp_data_dir)

    # Ensure flag doesn't exist
    if handler.setup_complete_flag.exists():
        handler.setup_complete_flag.unlink()

    # Mock successful subprocess.run
    mock_completed = SimpleNamespace(stdout="ok")
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *args, **kwargs: mock_completed,
    )

    handler.check_and_install_prerequisites()

    # Flag file should be created
    assert handler.setup_complete_flag.exists()


def test_check_and_install_calledprocesserror_exits(temp_data_dir, monkeypatch):
    handler = SetupHandler(data_dir=temp_data_dir)

    def raise_cpe(*a, **k):
        raise subprocess.CalledProcessError(1, "pip", stderr="boom")

    monkeypatch.setattr(subprocess, "run", raise_cpe)

    # Capture sys.exit
    exit_called = {}

    def fake_exit(code):
        exit_called["code"] = code
        raise SystemExit(code)

    monkeypatch.setattr(sys, "exit", fake_exit)

    with pytest.raises(SystemExit) as exc:
        handler.check_and_install_prerequisites()

    assert exit_called.get("code") == 1
    assert exc.value.code == 1


def test_check_and_install_filenotfound_exits(temp_data_dir, monkeypatch):
    handler = SetupHandler(data_dir=temp_data_dir)

    def raise_fnf(*a, **k):
        raise FileNotFoundError("requirements.txt not found")

    monkeypatch.setattr(subprocess, "run", raise_fnf)

    # Capture sys.exit
    exit_called = {}

    def fake_exit(code):
        exit_called["code"] = code
        raise SystemExit(code)

    monkeypatch.setattr(sys, "exit", fake_exit)

    with pytest.raises(SystemExit) as exc:
        handler.check_and_install_prerequisites()

    assert exit_called.get("code") == 1
    assert exc.value.code == 1


def test_run_all_checks_calls_both(monkeypatch, temp_data_dir):
    handler = SetupHandler(data_dir=temp_data_dir)
    h1 = MagicMock()
    h2 = MagicMock()
    monkeypatch.setattr(handler, "check_and_install_prerequisites", h1)
    monkeypatch.setattr(handler, "validate_ollama_setup", h2)

    handler.run_all_checks()

    h1.assert_called_once()
    h2.assert_called_once()


class FakeOllama:
    def __init__(self, running=True, model_available=True):
        self._running = running
        self._available = model_available
        self.model = "llama3"
        self.pulled = False

    def is_running(self):
        return self._running

    def is_model_available(self):
        return self._available

    def pull_model(self):
        self.pulled = True


def test_validate_ollama_not_running_returns(monkeypatch, temp_data_dir):
    # Force constructor to use our fake
    monkeypatch.setattr(sh, "OllamaHandler", lambda: FakeOllama(running=False))

    handler = SetupHandler(data_dir=temp_data_dir)

    # Should just return without raising or prompting
    handler.validate_ollama_setup()


def test_validate_ollama_model_available_no_prompt(monkeypatch, temp_data_dir):
    monkeypatch.setattr(sh, "OllamaHandler", lambda: FakeOllama(running=True, model_available=True))
    handler = SetupHandler(data_dir=temp_data_dir)

    # Spy on console.input to ensure it's not called
    input_spy = MagicMock()
    monkeypatch.setattr(sh.console, "input", input_spy)

    handler.validate_ollama_setup()

    input_spy.assert_not_called()


def test_validate_ollama_model_missing_user_declines(monkeypatch, temp_data_dir):
    fake = FakeOllama(running=True, model_available=False)
    monkeypatch.setattr(sh, "OllamaHandler", lambda: fake)
    handler = SetupHandler(data_dir=temp_data_dir)

    # User says 'n' to download
    monkeypatch.setattr(sh.console, "input", lambda prompt='': 'n')

    handler.validate_ollama_setup()

    assert fake.pulled is False


def test_validate_ollama_model_missing_user_accepts(monkeypatch, temp_data_dir):
    fake = FakeOllama(running=True, model_available=False)
    monkeypatch.setattr(sh, "OllamaHandler", lambda: fake)
    handler = SetupHandler(data_dir=temp_data_dir)

    # User says 'y' -> pull_model should be called
    monkeypatch.setattr(sh.console, "input", lambda prompt='': 'y')

    handler.validate_ollama_setup()

    assert fake.pulled is True
