"""Data structures for SEC company information."""
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, field, asdict
from datetime import datetime, date
from enum import Enum

class SwapType(str, Enum):
    """Types of swaps."""
    CREDIT_DEFAULT = "credit_default"
    INTEREST_RATE = "interest_rate"
    TOTAL_RETURN = "total_return"
    CURRENCY = "currency"
    COMMODITY = "commodity"
    EQUITY = "equity"
    OTHER = "other"

class PaymentFrequency(str, Enum):
    """Payment frequency for swap payments."""
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    SEMI_ANNUAL = "semi_annual"
    ANNUAL = "annual"
    MATURITY = "at_maturity"

@dataclass
class SwapContract:
    """Represents a single swap contract."""
    contract_id: str
    counterparty: str
    reference_entity: str
    notional_amount: float
    effective_date: Union[date, str]
    maturity_date: Union[date, str]
    currency: str = "USD"
    swap_type: Union[SwapType, str] = SwapType.OTHER
    payment_frequency: Union[PaymentFrequency, str] = PaymentFrequency.QUARTERLY
    fixed_rate: Optional[float] = None
    floating_rate_index: Optional[str] = None
    floating_rate_spread: Optional[float] = None
    collateral_terms: Dict[str, Any] = field(default_factory=dict)
    additional_terms: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        # Convert string dates to date objects if needed
        if isinstance(self.effective_date, str):
            self.effective_date = datetime.strptime(self.effective_date, "%Y-%m-%d").date()
        if isinstance(self.maturity_date, str):
            self.maturity_date = datetime.strptime(self.maturity_date, "%Y-%m-%d").date()
            
        # Convert enums from strings if needed
        if isinstance(self.swap_type, str):
            self.swap_type = SwapType(self.swap_type.lower())
        if isinstance(self.payment_frequency, str):
            self.payment_frequency = PaymentFrequency(self.payment_frequency.lower())
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert swap to dictionary."""
        data = asdict(self)
        # Convert enums to strings
        data['swap_type'] = self.swap_type.value
        data['payment_frequency'] = self.payment_frequency.value
        # Convert dates to ISO format strings
        if hasattr(self.effective_date, 'isoformat'):
            data['effective_date'] = self.effective_date.isoformat()
        if hasattr(self.maturity_date, 'isoformat'):
            data['maturity_date'] = self.maturity_date.isoformat()
        return data
        
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SwapContract':
        """Create SwapContract from dictionary."""
        return cls(**data)

@dataclass
class EntityIdentifiers:
    """Class for storing entity identifiers."""
    name: str
    cik: Optional[str] = None
    description: Optional[str] = None
    relationship_type: Optional[str] = None
    tickers: List[Dict[str, str]] = field(default_factory=list)
    filing_ids: List[str] = field(default_factory=list)

@dataclass
class CompanyInfo:
    """Represents identified company information with related entities."""
    name: str
    primary_identifiers: EntityIdentifiers
    related_entities: List[EntityIdentifiers] = field(default_factory=list)
