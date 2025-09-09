import pytest
from unittest.mock import MagicMock
from gamecock.swaps_analyzer import SwapsAnalyzer, SwapType
from gamecock.data_structures import SwapContract
from datetime import date
import pandas as pd

@pytest.fixture
def analyzer():
    """Create a SwapsAnalyzer instance with mock handlers."""
    db_handler = MagicMock()
    ollama_handler = MagicMock()
    return SwapsAnalyzer(db_handler=db_handler, ollama_handler=ollama_handler)

def test_calculate_risk_score_low_risk(analyzer):
    """Test risk score calculation for a low-risk scenario."""
    score = analyzer._calculate_risk_score(
        total_notional=1_000_000, 
        avg_time_to_maturity=2, 
        counterparty_concentration=0.3, 
        currency_concentration=0.5,
        swap_types=[SwapType.INTEREST_RATE]
    )
    assert 0 <= score <= 100
    assert score < 40  # Expecting low risk

def test_calculate_risk_score_medium_risk(analyzer):
    """Test risk score calculation for a medium-risk scenario."""
    score = analyzer._calculate_risk_score(
        total_notional=50_000_000, 
        avg_time_to_maturity=1, 
        counterparty_concentration=0.8, 
        currency_concentration=0.9,
        swap_types=[SwapType.EQUITY, SwapType.TOTAL_RETURN]
    )
    assert 0 <= score <= 100
    assert 40 <= score < 70  # Expecting medium risk

def test_calculate_risk_score_high_risk(analyzer):
    """Test risk score calculation for a high-risk scenario."""
    score = analyzer._calculate_risk_score(
        total_notional=100_000_000, 
        avg_time_to_maturity=0.5, 
        counterparty_concentration=0.95, 
        currency_concentration=1.0,
        swap_types=[SwapType.CREDIT_DEFAULT]
    )
    assert 0 <= score <= 100
    assert score >= 70  # Expecting high risk

def test_calculate_risk_score_zero_values(analyzer):
    """Test risk score calculation with zero values."""
    score = analyzer._calculate_risk_score(
        total_notional=0, 
        avg_time_to_maturity=0, 
        counterparty_concentration=0, 
        currency_concentration=0,
        swap_types=[]
    )
    assert score == 0

def test_generate_risk_report_success(analyzer):
    """Test successful generation of a risk report."""
    mock_swap = SwapContract(
        contract_id='test_swap',
        counterparty='Test CP',
        reference_entity='TEST',
        notional_amount=1000000,
        effective_date=date(2023, 1, 1),
        maturity_date=date(2025, 1, 1),
        swap_type=SwapType.INTEREST_RATE
    )
    
    mock_exposure = {
        'swaps': [mock_swap],
        'total_notional': 1000000,
        'num_swaps': 1,
        'exposure_by_counterparty': {'Test CP': 1000000},
        'exposure_by_currency': {'USD': 1000000},
        'earliest_maturity': '2025-01-01',
        'latest_maturity': '2025-01-01',
        'swap_types': [SwapType.INTEREST_RATE],
        'exposure_by_type': {SwapType.INTEREST_RATE.value: 1000000}
    }

    analyzer.calculate_exposure = MagicMock(return_value=mock_exposure)
    
    report = analyzer.generate_risk_report('TEST')

    assert 'error' not in report
    assert report['reference_entity'] == 'TEST'
    assert report['risk_score'] is not None
    assert report['risk_level'] is not None
    assert report['total_notional'] == 1000000

def test_calculate_exposure_success(analyzer):
    """Test successful calculation of exposure."""
    mock_swap_dicts = [
        {
            'contract_id': 'swap1',
            'counterparty': 'CP1',
            'reference_entity': 'ACME',
            'notional_amount': 1000000,
            'currency': 'USD',
            'swap_type': 'INTEREST_RATE',
            'effective_date': '2023-01-01',
            'maturity_date': '2028-01-01',
            'payment_frequency': 'QUARTERLY',
            'fixed_rate': 1.5,
            'floating_rate_index': 'SOFR',
            'floating_rate_spread': 0.5,
            'collateral_terms': '{}',
            'additional_terms': '{}'
        },
        {
            'contract_id': 'swap2',
            'counterparty': 'CP2',
            'reference_entity': 'ACME',
            'notional_amount': 2000000,
            'currency': 'USD',
            'swap_type': 'INTEREST_RATE',
            'effective_date': '2023-01-01',
            'maturity_date': '2030-01-01',
            'payment_frequency': 'SEMI_ANNUAL',
            'fixed_rate': 1.8,
            'floating_rate_index': 'SOFR',
            'floating_rate_spread': 0.6,
            'collateral_terms': '{}',
            'additional_terms': '{}'
        }
    ]
    analyzer.db.find_swaps_by_reference_entity.return_value = mock_swap_dicts

    exposure = analyzer.calculate_exposure('ACME')

    assert exposure['reference_entity'] == 'ACME'
    assert exposure['total_notional'] == 3000000
    assert exposure['num_swaps'] == 2
    assert len(exposure['counterparties']) == 2
    assert exposure['exposure_by_counterparty']['CP1'] == 1000000


def test_calculate_exposure_no_swaps(analyzer):
    """Test exposure calculation when no swaps are found."""
    analyzer.db.find_swaps_by_reference_entity.return_value = []

    exposure = analyzer.calculate_exposure('NON_EXISTENT')

    assert exposure == {}

def test_get_all_swaps_from_db_caching(analyzer):
    """Test that get_all_swaps_from_db caches results."""
    mock_swap_dicts = [
        {
            'contract_id': 'swap1',
            'counterparty': 'CP1',
            'reference_entity': 'ACME',
            'notional_amount': 1000000,
            'currency': 'USD',
            'swap_type': 'INTEREST_RATE',
            'effective_date': '2023-01-01',
            'maturity_date': '2028-01-01',
            'payment_frequency': 'QUARTERLY',
            'fixed_rate': 1.5,
            'floating_rate_index': 'SOFR',
            'floating_rate_spread': 0.5,
            'collateral_terms': '{}',
            'additional_terms': '{}'
        }
    ]
    analyzer.db.get_swap_obligations_view.return_value = mock_swap_dicts

    # First call, should hit the DB
    swaps1 = analyzer.get_all_swaps_from_db()
    assert len(swaps1) == 1
    analyzer.db.get_swap_obligations_view.assert_called_once()

    # Second call, should use cache
    swaps2 = analyzer.get_all_swaps_from_db()
    assert len(swaps2) == 1
    analyzer.db.get_swap_obligations_view.assert_called_once()  # Should not be called again


def test_clear_cache(analyzer):
    """Test that clear_cache clears the cache."""
    analyzer.db.get_swap_obligations_view.return_value = []

    analyzer.get_all_swaps_from_db()  # Populate cache
    analyzer.db.get_swap_obligations_view.assert_called_once()

    analyzer.clear_cache()

    analyzer.get_all_swaps_from_db()  # Should hit DB again
    assert analyzer.db.get_swap_obligations_view.call_count == 2


def test_get_all_swaps_from_db_error(analyzer):
    """Test error handling in get_all_swaps_from_db."""
    analyzer.db.get_swap_obligations_view.side_effect = Exception("DB Error")

    swaps = analyzer.get_all_swaps_from_db()

    assert swaps == []

def test_analyze_counterparty_risk_success(analyzer):
    """Test successful analysis of counterparty risk."""
    mock_swaps = [
        SwapContract(
            contract_id='swap1', counterparty='RISKY_CP', reference_entity='ACME', 
            notional_amount=1000000, swap_type=SwapType.CREDIT_DEFAULT, 
            effective_date=date.today(), maturity_date=date(2030, 1, 1)
        ),
        SwapContract(
            contract_id='swap2', counterparty='RISKY_CP', reference_entity='XYZ', 
            notional_amount=500000, swap_type=SwapType.INTEREST_RATE, 
            effective_date=date.today(), maturity_date=date(2025, 1, 1)
        ),
        SwapContract(
            contract_id='swap3', counterparty='SAFE_CP', reference_entity='ACME', 
            notional_amount=2000000, swap_type=SwapType.INTEREST_RATE, 
            effective_date=date.today(), maturity_date=date(2026, 1, 1)
        )
    ]
    analyzer.get_all_swaps_from_db = MagicMock(return_value=mock_swaps)

    report = analyzer.analyze_counterparty_risk('RISKY_CP')

    assert 'error' not in report
    assert report['counterparty'] == 'RISKY_CP'
    assert report['total_notional_exposure'] == 1500000
    assert report['num_contracts'] == 2
    assert len(report['reference_entities']) == 2

def test_analyze_counterparty_risk_no_swaps(analyzer):
    """Test counterparty risk analysis when no swaps are found."""
    analyzer.get_all_swaps_from_db = MagicMock(return_value=[])

    report = analyzer.analyze_counterparty_risk('GHOST_CP')

    assert 'error' in report
    assert report['error'] == 'No swaps found for counterparty: GHOST_CP'

@pytest.mark.parametrize(
    "score,expected_level",
    [
        (10, "Minimal"),
        (20, "Low"),
        (40, "Moderate"),
        (60, "High"),
        (80, "Very High"),
    ]
)
def test_get_risk_level(analyzer, score, expected_level):
    """Test the _get_risk_level method for all risk levels."""
    assert analyzer._get_risk_level(score) == expected_level

def test_export_to_csv_success(analyzer, tmp_path, monkeypatch):
    """Test successful export of swaps data to a CSV file."""
    mock_swaps = [
        SwapContract(
            contract_id='swap1', counterparty='CP1', reference_entity='ACME',
            notional_amount=1000000, currency='USD', swap_type=SwapType.INTEREST_RATE,
            effective_date=date(2023, 1, 1), maturity_date=date(2028, 1, 1),
            payment_frequency='QUARTERLY', fixed_rate=1.5, floating_rate_index='SOFR',
            floating_rate_spread=0.5, collateral_terms={},
            additional_terms={}
        )
    ]
    analyzer.get_all_swaps_from_db = MagicMock(return_value=mock_swaps)
    output_path = tmp_path / "swaps.csv"

    # Mock pandas to_csv to avoid actual file writing if needed, though tmp_path is safe
    mock_to_csv = MagicMock()
    monkeypatch.setattr(pd.DataFrame, 'to_csv', mock_to_csv)

    result = analyzer.export_to_csv(str(output_path))

    assert result is True
    mock_to_csv.assert_called_once()

@pytest.mark.parametrize(
    "risk_score,expected_level",
    [
        (30, "Low"),
        (60, "Medium"),
        (80, "High"),
    ]
)
def test_generate_risk_report_risk_levels(analyzer, risk_score, expected_level):
    """Test that generate_risk_report assigns the correct risk level."""
    mock_swap = MagicMock()
    mock_swap.maturity_date = date(2030, 1, 1)
    mock_exposure = {
        'swaps': [mock_swap], 'total_notional': 1, 'num_swaps': 1,
        'exposure_by_counterparty': {'CP1': 1}, 'exposure_by_currency': {'USD': 1},
        'earliest_maturity': '2025-01-01', 'latest_maturity': '2025-01-01',
        'swap_types': [], 'exposure_by_type': {}
    }
    analyzer.calculate_exposure = MagicMock(return_value=mock_exposure)
    analyzer._calculate_risk_score = MagicMock(return_value=risk_score)

    report = analyzer.generate_risk_report('TEST')

    assert report['risk_level'] == expected_level



def test_generate_risk_report_with_ai_summary(analyzer):
    """Test risk report generation with an AI summary."""
    mock_swap = SwapContract(
        contract_id='test_swap', counterparty='Test CP', reference_entity='TEST',
        notional_amount=1000000, swap_type=SwapType.INTEREST_RATE,
        effective_date=date.today(), maturity_date=date(2025, 1, 1)
    )
    mock_exposure = {
        'swaps': [mock_swap], 'total_notional': 1000000, 'num_swaps': 1,
        'exposure_by_counterparty': {'Test CP': 1000000}, 'exposure_by_currency': {'USD': 1000000},
        'earliest_maturity': '2025-01-01', 'latest_maturity': '2025-01-01',
        'swap_types': [SwapType.INTEREST_RATE], 'exposure_by_type': {SwapType.INTEREST_RATE.value: 1000000}
    }
    analyzer.calculate_exposure = MagicMock(return_value=mock_exposure)
    analyzer.ollama.is_running.return_value = True
    analyzer.ollama.is_model_available.return_value = True
    analyzer.ollama.generate.return_value = "This is an AI summary."

    report = analyzer.generate_risk_report('TEST', include_analysis=True)

    assert 'ai_summary' in report
    assert report['ai_summary'] == "This is an AI summary."
    analyzer.ollama.generate.assert_called_once()

def test_explain_swap_success(analyzer):
    """Test successful generation of a swap explanation."""
    mock_swap_details = [{
        'contract_id': 'swap1',
        'swap_type': 'INTEREST_RATE',
        'notional_amount': 1000000,
        'currency': 'USD',
        'counterparty': 'Test Counterparty',
        'reference_entity': 'Test Entity',
        'effective_date': '2023-01-01',
        'maturity_date': '2025-01-01',
        'obligation_id': 'ob1',
        'obligation_type': 'PAYMENT',
        'obligation_amount': 50000,
        'obligation_currency': 'USD',
        'due_date': '2024-01-01',
        'trigger_condition': 'N/A'
    }]
    analyzer.db.get_swap_obligations_view.return_value = mock_swap_details
    analyzer.ollama.is_running.return_value = True
    analyzer.ollama.is_model_available.return_value = True
    analyzer.ollama.generate.return_value = "This is a swap explanation."

    explanation = analyzer.explain_swap('swap1')

    assert explanation == "This is a swap explanation."
    # Check that the prompt was generated correctly
    call_args, call_kwargs = analyzer.ollama.generate.call_args
    prompt = call_args[0]
    assert "- **Notional Amount:** USD 1,000,000.00" in prompt
    assert "- **Counterparty:** Test Counterparty" in prompt


def test_explain_swap_ollama_unavailable(analyzer):
    """Test swap explanation when Ollama is not available."""
    analyzer.ollama.is_running.return_value = False

    explanation = analyzer.explain_swap('swap1')

    assert "Ollama service is not available" in explanation

def test_explain_swap_not_found(analyzer):
    """Test explaining a swap that does not exist."""
    analyzer.db.get_swap_obligations_view.return_value = []
    analyzer.ollama.is_running.return_value = True
    analyzer.ollama.is_model_available.return_value = True

    explanation = analyzer.explain_swap('non_existent_swap')

    assert "No swap found with Contract ID: non_existent_swap" in explanation

def test_explain_swap_generation_error(analyzer):
    """Test handling of exceptions during swap explanation generation."""
    mock_swap_details = [{
        'contract_id': 'swap1',
        'swap_type': 'INTEREST_RATE',
        'notional_amount': 1000000,
        'currency': 'USD',
    }]
    analyzer.db.get_swap_obligations_view.return_value = mock_swap_details
    analyzer.ollama.is_running.return_value = True
    analyzer.ollama.is_model_available.return_value = True
    analyzer.ollama.generate.side_effect = Exception("Generation failed")

    explanation = analyzer.explain_swap('swap1')

    assert "An error occurred while generating the explanation." in explanation

def test_format_obligations_for_prompt_no_obligations(analyzer):
    """Test formatting of an empty list of obligations."""
    formatted_text = analyzer._format_obligations_for_prompt([])
    assert formatted_text == "- No specific obligations listed."

def test_export_to_csv_no_swaps(analyzer):
    """Test exporting to CSV when there are no swaps to export."""
    analyzer.get_all_swaps_from_db = MagicMock(return_value=[])
    result = analyzer.export_to_csv('dummy_path.csv')
    assert result is False

def test_export_to_csv_exception(analyzer, monkeypatch):
    """Test exception handling during CSV export."""
    mock_swaps = [MagicMock()]
    analyzer.get_all_swaps_from_db = MagicMock(return_value=mock_swaps)
    
    mock_to_csv = MagicMock(side_effect=Exception("Disk full"))
    monkeypatch.setattr(pd.DataFrame, 'to_csv', mock_to_csv)
    
    result = analyzer.export_to_csv('dummy_path.csv')
    
    assert result is False

def test_generate_risk_report_no_swaps(analyzer):
    """Test risk report generation when no swaps are found."""
    analyzer.calculate_exposure = MagicMock(return_value={})
    
    report = analyzer.generate_risk_report('NON_EXISTENT')

    assert 'error' in report
    assert report['error'] == 'No swaps found for reference entity: NON_EXISTENT'

def test_generate_risk_report_no_ai_summary(analyzer):
    """Test risk report generation with AI summary when Ollama is not available."""
    mock_swap = SwapContract(
        contract_id='test_swap', counterparty='Test CP', reference_entity='TEST',
        notional_amount=1000000, swap_type=SwapType.INTEREST_RATE,
        effective_date=date.today(), maturity_date=date(2025, 1, 1)
    )
    mock_exposure = {
        'swaps': [mock_swap], 'total_notional': 1000000, 'num_swaps': 1,
        'exposure_by_counterparty': {'Test CP': 1000000}, 'exposure_by_currency': {'USD': 1000000},
        'earliest_maturity': '2025-01-01', 'latest_maturity': '2025-01-01',
        'swap_types': [SwapType.INTEREST_RATE], 'exposure_by_type': {SwapType.INTEREST_RATE.value: 1000000}
    }
    analyzer.calculate_exposure = MagicMock(return_value=mock_exposure)
    analyzer.ollama.is_running.return_value = False

    report = analyzer.generate_risk_report('TEST', include_analysis=True)

    assert 'ai_summary' in report
    assert report['ai_summary'] == "Ollama service not available for AI summary."
