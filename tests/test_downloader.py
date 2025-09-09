"""Tests for the downloader module (SECDownloader)."""

import os
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import gamecock.downloader as dl
from gamecock.downloader import SECDownloader


class FakeResponse:
    def __init__(self, json_data=None, content=b"", status_code=200, text=""):
        self._json = json_data
        self.content = content
        self.status_code = status_code
        self.text = text

    def json(self):
        return self._json


@pytest.fixture(autouse=True)
def ensure_user_agent(monkeypatch):
    # Ensure SEC_USER_AGENT is set for all tests
    monkeypatch.setenv("SEC_USER_AGENT", "TestAgent/1.0 test@example.com")


def test_init_raises_without_user_agent(monkeypatch):
    # Ensure that even if a local .env sets the var, we simulate it missing
    real_getenv = os.getenv
    monkeypatch.setattr(
        dl.os,
        "getenv",
        lambda key, default=None: None if key == "SEC_USER_AGENT" else real_getenv(key, default),
    )
    with pytest.raises(ValueError):
        SECDownloader()


def test_get_company_filings_no_response_returns_empty(monkeypatch):
    d = SECDownloader()
    d.session = MagicMock()
    monkeypatch.setattr(d, "_make_request", lambda url: None)
    res = d.get_company_filings("123", dl.datetime(2023, 1, 1), dl.datetime(2023, 12, 31))
    assert res == []


def test_get_company_filings_filters_and_handles_files(monkeypatch):
    d = SECDownloader()
    d.session = MagicMock()

    # Mock submissions with two filings; one in range, one out of range
    submissions = {
        "filings": {
            "recent": {
                "filingDate": ["2023-06-01", "2022-01-01"],
                "accessionNumber": ["0001-23-000001", "0001-22-000001"],
                "form": ["10-K", "10-Q"],
                "isXBRL": [True, False],
                "isInlineXBRL": [False, False],
                "primaryDocument": ["a.htm", "b.htm"],
                "fileNumber": ["1-1", "1-2"],
                "filmNumber": ["111", "222"],
                "size": [100, 200],
            }
        }
    }

    def fake_make_request(url):
        if url.endswith(".json") and "submissions" in url:
            return FakeResponse(json_data=submissions)
        # For directory listing of files
        return FakeResponse(json_data={
            "directory": {"item": [
                {"name": "a.htm", "type": "file", "size": 10, "last_modified": "now"},
                {"name": "x", "type": "dir"},
            ]}
        })

    monkeypatch.setattr(d, "_make_request", fake_make_request)

    res = d.get_company_filings("123", dl.datetime(2023, 1, 1), dl.datetime(2023, 12, 31), filing_types=["10-K"])
    assert len(res) == 1
    assert res[0]["form_type"] == "10-K"
    assert res[0]["files"][0]["name"] == "a.htm"


def test_get_filing_files_handles_missing_and_entries(monkeypatch):
    d = SECDownloader()
    d.session = MagicMock()

    # No response
    monkeypatch.setattr(d, "_make_request", lambda url: None)
    assert d.get_filing_files("123", "0001") == []

    # Response with empty directory
    monkeypatch.setattr(d, "_make_request", lambda url: FakeResponse(json_data={"directory": {}}))
    assert d.get_filing_files("123", "0001") == []

    # Response with entries, skipping dirs and nameless
    def resp_with_entries(url):
        return FakeResponse(json_data={
            "directory": {"item": [
                {"type": "dir"},
                {"name": None},
                {"name": "file1.txt", "type": "file", "size": 1, "last_modified": "t"},
            ]}
        })

    monkeypatch.setattr(d, "_make_request", resp_with_entries)
    files = d.get_filing_files("123", "0001")
    assert [f["name"] for f in files] == ["file1.txt"]


def test_download_filing_writes_files(tmp_path, monkeypatch):
    d = SECDownloader()
    d.session = MagicMock()

    # Fake response for file content
    content = b"hello"
    monkeypatch.setattr(d, "_make_request", lambda url: FakeResponse(content=content))

    files = [
        {"name": "a.txt", "type": "file", "size": 5, "last_modified": "now"},
        {"name": "b.txt", "type": "file", "size": 5, "last_modified": "now"},
    ]

    out = d.download_filing("123", "0001", output_dir=tmp_path, files=files)
    assert set(out.keys()) == {"a.txt", "b.txt"}
    for p in out.values():
        assert p.exists() and p.stat().st_size == len(content)


def test_download_company_filings_integration(tmp_path, monkeypatch):
    # Provide mocked db and swaps classes to avoid side effects
    db = MagicMock()
    db.upsert_filing = MagicMock()
    swaps_analyzer = MagicMock()
    swaps_processor = MagicMock()

    d = SECDownloader(output_dir=tmp_path, db_handler=db, swaps_analyzer=swaps_analyzer)
    # Replace internally constructed swaps_processor
    d.swaps_processor = swaps_processor
    d.session = MagicMock()

    # Filings list with one filing and precomputed files
    filing = {
        "accession_number": "0001",
        "filing_date": "2023-06-01",
        "form_type": "10-K",
        "files": [{"name": "a.txt", "type": "file", "size": 1, "last_modified": "t"}],
    }
    monkeypatch.setattr(d, "get_company_filings", lambda *a, **k: [filing])

    # download_filing returns a dict mapping name->path
    def fake_download(*a, **k):
        p = tmp_path / "123" / "0001" / "a.txt"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(b"x")
        return {"a.txt": p}

    monkeypatch.setattr(d, "download_filing", fake_download)

    res = d.download_company_filings("123", dl.datetime(2023, 1, 1), dl.datetime(2023, 12, 31))

    assert "0001" in res
    assert len(res["0001"]) == 1
    swaps_processor.process_filing.assert_called_once()
    db.upsert_filing.assert_called()


def test_download_company_filings_no_filings_returns_empty(tmp_path, monkeypatch):
    d = SECDownloader(output_dir=tmp_path)
    d.session = MagicMock()
    monkeypatch.setattr(d, "get_company_filings", lambda *a, **k: [])
    res = d.download_company_filings("123", dl.datetime(2023, 1, 1), dl.datetime(2023, 12, 31))
    assert res == {}


def test_download_company_filings_records_filing_metadata(tmp_path, monkeypatch):
    # Prepare handler with mocked DB
    db = MagicMock()
    db.upsert_filing = MagicMock()

    d = SECDownloader(output_dir=tmp_path, db_handler=db)
    d.session = MagicMock()

    filing = {
        "accession_number": "0002",
        "filing_date": "2023-05-01",
        "form_type": "10-Q",
        "files": [{"name": "x.txt", "type": "file", "size": 1, "last_modified": "t"}],
    }
    monkeypatch.setattr(d, "get_company_filings", lambda *a, **k: [filing])

    def fake_download(*a, **k):
        p = tmp_path / "123" / "0002" / "x.txt"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(b"x")
        return {"x.txt": p}

    monkeypatch.setattr(d, "download_filing", fake_download)

    res = d.download_company_filings("123", dl.datetime(2023, 1, 1), dl.datetime(2023, 12, 31))
    assert "0002" in res
    db.upsert_filing.assert_called_with(
        company_cik="123",
        accession_number="0002",
        form_type="10-Q",
        filing_date="2023-05-01",
        file_path=str(tmp_path / "123" / "0002"),
    )


def test_download_filing_request_failure_returns_empty(tmp_path, monkeypatch):
    d = SECDownloader(output_dir=tmp_path)
    d.session = MagicMock()
    monkeypatch.setattr(d, "_make_request", lambda url: None)
    files = [{"name": "a.txt", "type": "file", "size": 1, "last_modified": "t"}]
    res = d.download_filing("123", "0003", output_dir=tmp_path, files=files)
    assert res == {}


def test_download_filing_write_failure_cleanup(tmp_path, monkeypatch):
    d = SECDownloader(output_dir=tmp_path)
    d.session = MagicMock()

    # Will create file then raise to trigger cleanup
    content = b"data"
    monkeypatch.setattr(d, "_make_request", lambda url: FakeResponse(content=content))

    target_dir = tmp_path / "123" / "0004"
    target_dir.mkdir(parents=True, exist_ok=True)
    target_file = target_dir / "a.txt"

    # Custom open that creates the file then raises
    import builtins
    real_open = builtins.open
    def raising_open(path, mode="r", *args, **kwargs):
        if str(path).endswith("a.txt") and "wb" in mode:
            # create then raise
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            Path(path).touch()
            raise IOError("disk full")
        return real_open(path, mode, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", raising_open)

    res = d.download_filing("123", "0004", output_dir=target_dir, files=[{"name": "a.txt", "type": "file", "size": 1, "last_modified": "t"}])
    assert res == {}
    # Ensure file cleaned up
    assert not target_file.exists()


def test_download_filing_skips_existing_file(tmp_path, monkeypatch):
    d = SECDownloader(output_dir=tmp_path)
    d.session = MagicMock()

    target_dir = tmp_path / "123" / "0005"
    target_dir.mkdir(parents=True, exist_ok=True)
    existing = target_dir / "a.txt"
    existing.write_bytes(b"x")  # non-empty -> considered existing

    # _make_request should not be called; raise if it is
    def boom(url):
        raise AssertionError("_make_request should not be called for existing file")
    monkeypatch.setattr(d, "_make_request", boom)

    res = d.download_filing("123", "0005", output_dir=target_dir, files=[{"name": "a.txt", "type": "file", "size": 1, "last_modified": "t"}])
    assert "a.txt" in res and res["a.txt"].exists()


def test_get_company_filings_invalid_date_and_entry_error(monkeypatch):
    d = SECDownloader()
    d.session = MagicMock()

    submissions = {
        "filings": {
            "recent": {
                "filingDate": ["invalid-date"],
                "accessionNumber": ["0001-23-000001"],
                "form": ["10-K"],
                "isXBRL": [True],
                "isInlineXBRL": [False],
                "primaryDocument": ["a.htm"],
                "fileNumber": ["1-1"],
                "filmNumber": ["111"],
                "size": [100],
            }
        }
    }

    # Directory listing returns malformed entries including a non-dict
    def fake_make_request(url):
        if url.endswith(".json") and "submissions" in url:
            return FakeResponse(json_data=submissions)
        return FakeResponse(json_data={"directory": {"item": [[], {"name": "a.htm", "type": "file", "size": 1, "last_modified": "t"}]}})

    monkeypatch.setattr(d, "_make_request", fake_make_request)

    res = d.get_company_filings("123", dl.datetime(2023, 1, 1), dl.datetime(2023, 12, 31))
    # invalid date means no filings matched; still should not crash from entry error
    assert res == []
