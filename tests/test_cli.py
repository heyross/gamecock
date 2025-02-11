"""
Tests for the CLI module.
"""
import pytest
from click.testing import CliRunner
from pathlib import Path
from gamecock.cli import cli, download, search

@pytest.fixture
def runner():
    """Create a CLI runner."""
    return CliRunner()

def test_cli_version(runner):
    """Test the --version option."""
    result = runner.invoke(cli, ["--version"])
    assert result.exit_code == 0
    assert "version" in result.output.lower()

def test_download_command(runner, tmp_path):
    """Test the download command."""
    result = runner.invoke(download, ["--output", str(tmp_path)])
    assert result.exit_code == 0
    assert "Downloading to" in result.output

def test_download_command_default_dir(runner):
    """Test download command with default directory."""
    result = runner.invoke(download)
    assert result.exit_code == 0
    assert "downloads" in result.output

def test_search_command(runner, tmp_path):
    """Test the search command."""
    # Create a test file
    test_file = tmp_path / "test.txt"
    test_file.write_text("This is a test file\nwith test content")
    
    result = runner.invoke(search, ["test", "--dir", str(tmp_path)])
    assert result.exit_code == 0
    assert "test.txt" in result.output

def test_search_command_no_results(runner, tmp_path):
    """Test search command with no results."""
    result = runner.invoke(search, ["nonexistent", "--dir", str(tmp_path)])
    assert result.exit_code == 0
    # Table should be empty
    assert "─" in result.output  # Table border

def test_search_command_invalid_dir(runner):
    """Test search command with invalid directory."""
    result = runner.invoke(search, ["test", "--dir", "/nonexistent/dir"])
    assert result.exit_code != 0
    assert "Error" in result.output or "Invalid" in result.output
