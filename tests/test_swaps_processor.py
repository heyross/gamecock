"""Tests for the SwapsProcessor class."""
import pytest
from pathlib import Path
from unittest.mock import MagicMock

from gamecock.swaps_processor import SwapsProcessor
from gamecock.db_handler import DatabaseHandler

@pytest.fixture
def test_data_dir(tmp_path):
    """Create a temporary directory with test data files."""
    data_dir = tmp_path / "test_data"
    data_dir.mkdir()
    
    # Create sample CSV
    csv_content = """
contract_id,counterparty,reference_entity,notional_amount,currency,effective_date,maturity_date,swap_type,fixed_rate
SWAP001,Goldman Sachs,GME,1000000,USD,2023-01-01,2028-01-01,credit_default,1.5
SWAP002,JP Morgan,AMC,500000,USD,2023-02-01,2025-02-01,interest_rate,2.1
# Malformed row
SWAP006,Morgan Stanley,MSFT,invalid_notional,USD,2023-06-01,2029-06-01,interest_rate,2.5
"""
    (data_dir / "sample.csv").write_text(csv_content)
    
    # Create sample JSON
    json_content = """[
    {
        "contract_id": "SWAP007",
        "counterparty": "Deutsche Bank",
        "reference_entity": "DBX",
        "notional_amount": 3000000,
        "currency": "EUR",
        "effective_date": "2023-07-01",
        "maturity_date": "2033-07-01",
        "swap_type": "credit_default",
        "fixed_rate": 1.2
    }]
"""
    (data_dir / "sample.json").write_text(json_content)
    
    return data_dir

@pytest.fixture
def test_db():
    """Create a test database for the tests."""
    db_path = ":memory:"
    db = DatabaseHandler(db_url=f"sqlite:///{db_path}")
    yield db

class TestSwapsProcessor:
    """Test suite for the SwapsProcessor."""

    def test_process_csv_file(self, test_db, test_data_dir):
        """Test processing a valid CSV file."""
        processor = SwapsProcessor(db_handler=test_db)
        csv_file = test_data_dir / "sample.csv"
        
        swaps = processor.process_filing(csv_file, save_to_db=False)
        
        assert len(swaps) == 2
        assert swaps[0].contract_id == "SWAP001"
        assert swaps[1].counterparty == "JP Morgan"

    def test_process_json_file(self, test_db, test_data_dir):
        """Test processing a valid JSON file."""
        processor = SwapsProcessor(db_handler=test_db)
        json_file = test_data_dir / "sample.json"
        
        swaps = processor.process_filing(json_file, save_to_db=False)
        
        assert len(swaps) == 1
        assert swaps[0].contract_id == "SWAP007"
        assert swaps[0].notional_amount == 3000000

    def test_save_to_db(self, test_db, test_data_dir):
        """Test that processed swaps are correctly saved to the database."""
        processor = SwapsProcessor(db_handler=test_db)
        csv_file = test_data_dir / "sample.csv"
        
        processor.process_filing(csv_file, save_to_db=True)
        
        # Verify data was saved
        saved_swaps = test_db.find_swaps_by_reference_entity("GME")
        assert len(saved_swaps) == 1
        assert saved_swaps[0]['contract_id'] == "SWAP001"

        counterparties = test_db.get_all_counterparties()
        assert len(counterparties) == 2
        assert "Goldman Sachs" in [c['name'] for c in counterparties]

    def test_malformed_row_handling(self, test_db, test_data_dir):
        """Test that malformed rows are skipped without crashing."""
        processor = SwapsProcessor(db_handler=test_db)
        csv_file = test_data_dir / "sample.csv"
        
        # The malformed row should be skipped, and the valid rows processed
        swaps = processor.process_filing(csv_file, save_to_db=True)
        assert len(swaps) == 2

        # Verify that only the valid swaps were saved
        all_db_swaps = test_db.get_swap_obligations_view()
        assert len(all_db_swaps) == 2
