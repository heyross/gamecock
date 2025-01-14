"""
Shared test fixtures and configuration.
"""
import pytest
import logging
from pathlib import Path

@pytest.fixture(autouse=True)
def disable_logging():
    """Disable logging during tests."""
    logging.getLogger().setLevel(logging.ERROR)

@pytest.fixture
def sample_sec_data(tmp_path):
    """Create sample SEC filing data for testing."""
    data_dir = tmp_path / "sec_data"
    data_dir.mkdir()
    
    # Create sample 10-K file
    tenk_dir = data_dir / "10-K"
    tenk_dir.mkdir()
    (tenk_dir / "sample_10k.txt").write_text("""
UNITED STATES
SECURITIES AND EXCHANGE COMMISSION
Washington, D.C. 20549

FORM 10-K

ANNUAL REPORT PURSUANT TO SECTION 13 OR 15(d)
OF THE SECURITIES EXCHANGE ACT OF 1934

For the fiscal year ended December 31, 2024
    """.strip())
    
    # Create sample 10-Q file
    tenq_dir = data_dir / "10-Q"
    tenq_dir.mkdir()
    (tenq_dir / "sample_10q.txt").write_text("""
UNITED STATES
SECURITIES AND EXCHANGE COMMISSION
Washington, D.C. 20549

FORM 10-Q

QUARTERLY REPORT PURSUANT TO SECTION 13 OR 15(d)
OF THE SECURITIES EXCHANGE ACT OF 1934

For the quarterly period ended September 30, 2024
    """.strip())
    
    return data_dir

@pytest.fixture
def mock_sec_response():
    """Sample SEC API response data."""
    return {
        "filings": [
            {
                "accessionNumber": "0001234567-24-000123",
                "filingDate": "2024-01-13",
                "form": "10-K",
                "size": 1234567
            },
            {
                "accessionNumber": "0001234567-24-000124",
                "filingDate": "2024-01-13",
                "form": "10-Q",
                "size": 234567
            }
        ]
    }
