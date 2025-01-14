"""
SEC Forms definitions and documentation.
"""
from dataclasses import dataclass
from typing import Optional, List

@dataclass
class SECForm:
    """Represents an SEC form with its metadata."""
    name: str
    description: str
    investopedia_link: str
    filing_frequency: Optional[str] = None
    related_forms: List[str] = None

    def __post_init__(self):
        if self.related_forms is None:
            self.related_forms = []

# Core SEC Forms
FORM_10K = SECForm(
    name="10-K",
    description="Annual report providing comprehensive overview of the company's financial performance.",
    investopedia_link="https://www.investopedia.com/terms/1/10-k.asp",
    filing_frequency="Annual",
    related_forms=["10-K/A"]
)

FORM_10Q = SECForm(
    name="10-Q",
    description="Quarterly report providing financial performance update.",
    investopedia_link="https://www.investopedia.com/terms/1/10-q.asp",
    filing_frequency="Quarterly",
    related_forms=["10-Q/A"]
)

# Add more forms as needed...

# Form Groups for easier processing
ANNUAL_FORMS = [FORM_10K]
QUARTERLY_FORMS = [FORM_10Q]
