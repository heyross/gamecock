import pytest
from click.testing import CliRunner
from unittest.mock import MagicMock

import gamecock.cli as cli_mod
from gamecock.cli import cli


@pytest.fixture
def runner():
    return CliRunner()


def test_cli_menu_invokes_main_menu_with_debug(runner, monkeypatch):
    # Mock SetupHandler to avoid real checks
    setup_mock = MagicMock()
    monkeypatch.setattr(cli_mod, "SetupHandler", lambda: setup_mock)

    # Mock MenuSystem to avoid interactive loop
    menu_mock = MagicMock()
    menu_instance = MagicMock()
    menu_instance.main_menu = MagicMock()
    menu_mock.return_value = menu_instance
    monkeypatch.setattr(cli_mod, "MenuSystem", menu_mock)

    result = runner.invoke(cli, ["menu", "--debug"])  # enable debug path too

    assert result.exit_code == 0
    setup_mock.run_all_checks.assert_called_once()
    menu_instance.main_menu.assert_called_once()


def test_cli_analyze_success_with_ai_summary(runner, monkeypatch):
    # Mock SetupHandler
    setup_mock = MagicMock()
    monkeypatch.setattr(cli_mod, "SetupHandler", lambda: setup_mock)

    # Mock SwapsAnalyzer
    analyzer_instance = MagicMock()
    analyzer_instance.generate_risk_report.return_value = {
        "risk_score": 75.0,
        "risk_level": "High",
        "total_notional": 1_000_000.0,
        "num_swaps": 10,
        "ai_summary": "Summary text",
    }
    monkeypatch.setattr(cli_mod, "SwapsAnalyzer", lambda: analyzer_instance)

    result = runner.invoke(cli, ["analyze", "--entity", "CP1"])

    assert result.exit_code == 0
    analyzer_instance.generate_risk_report.assert_called_once()
    # Ensure summary printed
    assert "AI Summary:" in result.output


def test_cli_analyze_error_path(runner, monkeypatch):
    setup_mock = MagicMock()
    monkeypatch.setattr(cli_mod, "SetupHandler", lambda: setup_mock)

    analyzer_instance = MagicMock()
    analyzer_instance.generate_risk_report.return_value = {"error": "No data"}
    monkeypatch.setattr(cli_mod, "SwapsAnalyzer", lambda: analyzer_instance)

    result = runner.invoke(cli, ["analyze", "--entity", "UNKNOWN"])

    assert result.exit_code == 0
    analyzer_instance.generate_risk_report.assert_called_once()


def test_cli_explain_prints_output(runner, monkeypatch):
    setup_mock = MagicMock()
    monkeypatch.setattr(cli_mod, "SetupHandler", lambda: setup_mock)

    analyzer_instance = MagicMock()
    analyzer_instance.explain_swap.return_value = "Explanation text"
    monkeypatch.setattr(cli_mod, "SwapsAnalyzer", lambda: analyzer_instance)

    result = runner.invoke(cli, ["explain", "--contract", "c1"])

    assert result.exit_code == 0
    analyzer_instance.explain_swap.assert_called_once_with("c1")
    assert "Explanation" in result.output


def test_cli_startup_exception_causes_exit(runner, monkeypatch):
    # Force SetupHandler.run_all_checks to raise
    class BoomSetup:
        def run_all_checks(self):
            raise RuntimeError("boom")
    monkeypatch.setattr(cli_mod, "SetupHandler", lambda: BoomSetup())

    result = runner.invoke(cli, ["analyze", "--entity", "CP1"])  # any command triggers setup

    assert result.exit_code == 1
    assert "An unexpected error occurred during startup" in result.output
