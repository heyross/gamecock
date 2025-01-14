"""
Tests for the forms module.
"""
import pytest
from gamecock.forms import SECForm, FORM_10K, FORM_10Q

def test_sec_form_creation():
    """Test creating a SECForm instance."""
    form = SECForm(
        name="TEST-1",
        description="Test form",
        investopedia_link="https://example.com",
        filing_frequency="Annual"
    )
    assert form.name == "TEST-1"
    assert form.description == "Test form"
    assert form.investopedia_link == "https://example.com"
    assert form.filing_frequency == "Annual"
    assert form.related_forms == []

def test_sec_form_with_related():
    """Test SECForm with related forms."""
    form = SECForm(
        name="TEST-2",
        description="Test form 2",
        investopedia_link="https://example.com",
        related_forms=["TEST-2/A"]
    )
    assert form.related_forms == ["TEST-2/A"]

def test_predefined_forms():
    """Test predefined SEC forms."""
    assert FORM_10K.name == "10-K"
    assert FORM_10K.filing_frequency == "Annual"
    assert "10-K/A" in FORM_10K.related_forms
    
    assert FORM_10Q.name == "10-Q"
    assert FORM_10Q.filing_frequency == "Quarterly"
    assert "10-Q/A" in FORM_10Q.related_forms
