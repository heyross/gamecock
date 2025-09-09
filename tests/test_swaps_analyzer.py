import pytest
from unittest.mock import MagicMock
from gamecock.swaps_analyzer import SwapsAnalyzer

@pytest.fixture
def analyzer():
    """Create a SwapsAnalyzer instance with a mock DB handler."""
    db_handler = MagicMock()
    return SwapsAnalyzer(db_handler=db_handler)

def test_calculate_risk_score_low_risk(analyzer):
    """Test risk score calculation for a low-risk scenario."""
    score = analyzer._calculate_risk_score(total_notional=100000, num_swaps=5, num_counterparties=2)
    assert 0 <= score <= 100
    assert score < 40  # Expecting low risk

def test_calculate_risk_score_medium_risk(analyzer):
    """Test risk score calculation for a medium-risk scenario."""
    score = analyzer._calculate_risk_score(total_notional=5000000, num_swaps=50, num_counterparties=1)
    assert 0 <= score <= 100
    assert 40 <= score < 70  # Expecting medium risk

def test_calculate_risk_score_high_risk(analyzer):
    """Test risk score calculation for a high-risk scenario."""
    score = analyzer._calculate_risk_score(total_notional=20000000, num_swaps=100, num_counterparties=1)
    assert 0 <= score <= 100
    assert score >= 70  # Expecting high risk

def test_calculate_risk_score_zero_values(analyzer):
    """Test risk score calculation with zero values."""
    score = analyzer._calculate_risk_score(total_notional=0, num_swaps=0, num_counterparties=0)
    assert score == 0
