"""Data structures for SEC company information."""
from typing import Dict, List, Optional
from dataclasses import dataclass, field

@dataclass
class EntityIdentifiers:
    """Class for storing entity identifiers."""
    name: str
    cik: Optional[str] = None
    description: Optional[str] = None
    relationship: Optional[str] = None
    tickers: List[Dict[str, str]] = field(default_factory=list)
    filing_ids: List[str] = field(default_factory=list)

@dataclass
class CompanyInfo:
    """Represents identified company information with related entities."""
    name: str
    primary_identifiers: EntityIdentifiers
    related_entities: List[EntityIdentifiers] = field(default_factory=list)
