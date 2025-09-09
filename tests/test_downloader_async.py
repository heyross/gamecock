import os
from unittest.mock import MagicMock
import pytest

import gamecock.downloader as dl
from gamecock.downloader import SECDownloader


@pytest.fixture(autouse=True)
def ensure_user_agent(monkeypatch):
    monkeypatch.setenv("SEC_USER_AGENT", "TestAgent/1.0 test@example.com")


def test_download_company_filings_async_processing_submission(tmp_path, monkeypatch):
    # Enable async processing and ensure _submit_processing is used
    db = MagicMock()
    d = SECDownloader(output_dir=tmp_path, db_handler=db, process_async=True, max_workers=2)
    d.session = MagicMock()

    filing = {
        "accession_number": "0003",
        "filing_date": "2023-06-01",
        "form_type": "10-K",
        "files": [{"name": "a.csv", "type": "file", "size": 1, "last_modified": "t"}],
    }
    monkeypatch.setattr(d, "get_company_filings", lambda *a, **k: [filing])

    # Make download_filing return a csv path
    def fake_download(*a, **k):
        p = tmp_path / "123" / "0003" / "a.csv"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(
            "contract_id,counterparty,reference_entity,notional_amount,effective_date,maturity_date\n"
            "1,CP,ENT,100,2023-01-01,2024-01-01\n"
        )
        return {"a.csv": p}

    monkeypatch.setattr(d, "download_filing", fake_download)

    # Spy on _submit_processing to verify async submission path is taken
    spy = MagicMock()
    monkeypatch.setattr(d, "_submit_processing", spy)

    d.download_company_filings("123", dl.datetime(2023, 1, 1), dl.datetime(2023, 12, 31))

    spy.assert_called_once()

    # Ensure we clean up the executor threads in tests
    d.wait_for_processing()
