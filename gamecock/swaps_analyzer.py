"""
Swaps Analysis Module

This module provides functionality to analyze swaps data from various sources including SEC filings.
"""
import os
import logging
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, field, asdict
from datetime import datetime, date
import json
from enum import Enum

from .db_handler import DatabaseHandler
from .ollama_handler import OllamaHandler

logger = logging.getLogger(__name__)

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
        data['effective_date'] = self.effective_date.isoformat()
        data['maturity_date'] = self.maturity_date.isoformat()
        return data
        
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SwapContract':
        """Create SwapContract from dictionary."""
        return cls(**data)

class SwapsAnalyzer:
    """Handles analysis of swaps data from various sources."""
    
    def __init__(self, db_handler: Optional[DatabaseHandler] = None, ollama_handler: Optional[OllamaHandler] = None, data_dir: str = "data"):
        """Initialize the swaps analyzer.
        
        Args:
            db_handler: Database handler instance (optional)
            data_dir: Directory where swaps data files are stored
        """
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.db = db_handler or DatabaseHandler()
        self.ollama = ollama_handler or OllamaHandler()
        self._loaded_swaps: List[SwapContract] = []
        self._db_swaps_cache: Optional[List[SwapContract]] = None
        
    @property
    def swaps(self) -> List[SwapContract]:
        """Get all swaps from both memory and database."""
        return self._loaded_swaps + self._get_swaps_from_db()
    
    def _get_swaps_from_db(self) -> List[SwapContract]:
        """Load swaps from the database, using a cache."""
        if self._db_swaps_cache is not None:
            return self._db_swaps_cache
        
        try:
            swap_dicts = self.db.get_swap_obligations_view()
            self._db_swaps_cache = [SwapContract.from_dict(s) for s in swap_dicts]
            return self._db_swaps_cache
        except Exception as e:
            logger.error(f"Error loading swaps from database: {str(e)}")
            return []

    def clear_cache(self):
        """Clear the internal swaps cache."""
        self._loaded_swaps = []
        self._db_swaps_cache = None
        logger.info("Swaps analyzer cache has been cleared.")
        
    def load_swaps_from_directory(self, directory: Union[str, Path], save_to_db: bool = True):
        """Load all swap files from a directory."""
        directory = Path(directory)
        if not directory.is_dir():
            logger.error(f"Directory not found: {directory}")
            return

        for file_path in directory.glob('**/*'):
            if file_path.is_file() and file_path.suffix.lower() in ['.csv', '.json']:
                self.load_swaps_from_file(file_path, save_to_db=save_to_db)

    def load_swaps_from_file(self, file_path: Union[str, Path], save_to_db: bool = True) -> List[SwapContract]:
        """Load swaps data from a file."""
        loaded_swaps = []
        try:
            file_path = Path(file_path)
            if not file_path.exists():
                logger.error(f"File not found: {file_path}")
                return loaded_swaps

            logger.info(f"Loading swaps from {file_path}")

            if file_path.suffix.lower() == '.csv':
                df = pd.read_csv(file_path)
                loaded_swaps = self._process_dataframe(df)
            elif file_path.suffix.lower() == '.json':
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    loaded_swaps = self._process_json(data)
            else:
                logger.warning(f"Unsupported file format for swaps: {file_path.suffix}")

            if save_to_db and loaded_swaps:
                saved_count = self._save_swaps_to_db(loaded_swaps)
                if saved_count > 0:
                    self.clear_cache() # Force reload from DB on next access

            self._loaded_swaps.extend(loaded_swaps)
            logger.info(f"Successfully loaded {len(loaded_swaps)} swaps from {file_path}")

        except Exception as e:
            logger.error(f"Error loading swaps data from {file_path}: {str(e)}", exc_info=True)

        return loaded_swaps
        
    def _save_swaps_to_db(self, swaps: List[SwapContract]) -> int:
        """Save a list of swaps to the database, ensuring entities are created."""
        saved_count = 0
        for swap in swaps:
            try:
                # Ensure counterparty and reference_entity exist before saving swap
                self.db.swaps_db.get_or_create_counterparty(swap.counterparty)
                self.db.swaps_db.get_or_create_security(swap.reference_entity)

                swap_dict = swap.to_dict()
                saved_swap = self.db.save_swap(swap_dict)
                
                if saved_swap:
                    saved_count += 1
                    self._process_swap_details(saved_swap['id'], swap)
            except Exception as e:
                logger.error(f"Error saving swap {swap.contract_id} to database: {str(e)}")

        if saved_count > 0:
            logger.info(f"Successfully saved {saved_count} swaps to the database.")
            self.clear_cache() # Invalidate cache after DB changes

        return saved_count
    
    def _process_swap_details(self, swap_id: int, swap: SwapContract):
        """Process and save underlying instruments and obligations for a swap.
        
        Args:
            swap_id: Database ID of the saved swap
            swap: SwapContract object containing the swap details
        """
        try:
            # Extract and save underlying instruments
            instruments = self._extract_underlying_instruments(swap)
            for instrument_data in instruments:
                saved_instrument = self.db.add_underlying_instrument(swap_id, instrument_data)
                if saved_instrument:
                    logger.debug(f"Added underlying instrument {instrument_data['identifier']} to swap {swap.contract_id}")
            
            # Extract and save obligations
            obligations = self._extract_obligations(swap)
            for obligation_data in obligations:
                saved_obligation = self.db.add_obligation(swap_id, obligation_data)
                if saved_obligation:
                    logger.debug(f"Added obligation {obligation_data['obligation_type']} to swap {swap.contract_id}")
                    
                    # Extract and save triggers for this obligation
                    triggers = self._extract_obligation_triggers(swap, obligation_data)
                    for trigger_data in triggers:
                        saved_trigger = self.db.add_obligation_trigger(saved_obligation['id'], trigger_data)
                        if saved_trigger:
                            logger.debug(f"Added trigger {trigger_data['trigger_type']} to obligation {saved_obligation['id']}")
                            
        except Exception as e:
            logger.error(f"Error processing swap details for {swap.contract_id}: {str(e)}")
    
    def _extract_underlying_instruments(self, swap: SwapContract) -> List[Dict[str, Any]]:
        """Extract underlying instruments from a swap contract.
        
        Args:
            swap: SwapContract object
            
        Returns:
            List of dictionaries containing instrument data
        """
        instruments = []
        
        # Extract from reference entity (most common case)
        if swap.reference_entity:
            instrument = {
                'instrument_type': self._determine_instrument_type(swap.reference_entity),
                'identifier': swap.reference_entity,
                'description': f"Reference entity for {swap.swap_type.value} swap",
                'notional_amount': swap.notional_amount,
                'currency': swap.currency
            }
            instruments.append(instrument)
        
        # Extract from additional terms if available
        if swap.additional_terms and 'underlying_instruments' in swap.additional_terms:
            for instrument_info in swap.additional_terms['underlying_instruments']:
                if isinstance(instrument_info, dict):
                    instruments.append(instrument_info)
                elif isinstance(instrument_info, str):
                    # Simple string identifier
                    instrument = {
                        'instrument_type': self._determine_instrument_type(instrument_info),
                        'identifier': instrument_info,
                        'description': f"Underlying instrument for {swap.swap_type.value} swap",
                        'currency': swap.currency
                    }
                    instruments.append(instrument)
        
        return instruments
    
    def _extract_obligations(self, swap: SwapContract) -> List[Dict[str, Any]]:
        """Extract obligations from a swap contract.
        
        Args:
            swap: SwapContract object
            
        Returns:
            List of dictionaries containing obligation data
        """
        obligations = []
        
        # Generate standard payment obligations based on swap type
        if swap.swap_type == SwapType.INTEREST_RATE:
            # Fixed rate payer obligation
            if swap.fixed_rate:
                obligation = {
                    'obligation_type': 'fixed_payment',
                    'amount': swap.notional_amount * (swap.fixed_rate / 100) / self._get_payment_frequency_factor(swap.payment_frequency),
                    'currency': swap.currency,
                    'due_date': self._calculate_next_payment_date(swap.effective_date, swap.payment_frequency),
                    'status': 'pending',
                    'description': f"Fixed rate payment at {swap.fixed_rate}%"
                }
                obligations.append(obligation)
            
            # Floating rate receiver obligation
            if swap.floating_rate_index:
                obligation = {
                    'obligation_type': 'floating_payment',
                    'amount': 0,  # To be calculated based on floating rate
                    'currency': swap.currency,
                    'due_date': self._calculate_next_payment_date(swap.effective_date, swap.payment_frequency),
                    'status': 'pending',
                    'description': f"Floating rate payment based on {swap.floating_rate_index}"
                }
                obligations.append(obligation)
        
        elif swap.swap_type == SwapType.CREDIT_DEFAULT:
            # Premium payment obligation
            obligation = {
                'obligation_type': 'premium_payment',
                'amount': swap.notional_amount * 0.01,  # Default 1% premium, should be extracted from terms
                'currency': swap.currency,
                'due_date': self._calculate_next_payment_date(swap.effective_date, swap.payment_frequency),
                'status': 'pending',
                'description': "Credit default swap premium payment"
            }
            obligations.append(obligation)
            
            # Protection payment obligation (contingent)
            obligation = {
                'obligation_type': 'protection_payment',
                'amount': swap.notional_amount,
                'currency': swap.currency,
                'due_date': None,  # Triggered by credit event
                'status': 'contingent',
                'description': "Protection payment upon credit event"
            }
            obligations.append(obligation)
        
        elif swap.swap_type == SwapType.TOTAL_RETURN:
            # Total return payment obligation
            obligation = {
                'obligation_type': 'total_return_payment',
                'amount': 0,  # Variable based on asset performance
                'currency': swap.currency,
                'due_date': self._calculate_next_payment_date(swap.effective_date, swap.payment_frequency),
                'status': 'pending',
                'description': f"Total return payment on {swap.reference_entity}"
            }
            obligations.append(obligation)
        
        # Extract from additional terms if available
        if swap.additional_terms and 'obligations' in swap.additional_terms:
            for obligation_info in swap.additional_terms['obligations']:
                if isinstance(obligation_info, dict):
                    obligations.append(obligation_info)
        
        return obligations
    
    def _extract_obligation_triggers(self, swap: SwapContract, obligation: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract triggers for an obligation.
        
        Args:
            swap: SwapContract object
            obligation: Dictionary containing obligation data
            
        Returns:
            List of dictionaries containing trigger data
        """
        triggers = []
        
        # Generate triggers based on obligation type
        if obligation['obligation_type'] == 'protection_payment':
            # Credit event trigger for CDS
            trigger = {
                'trigger_type': 'credit_event',
                'trigger_condition': f"credit_event({swap.reference_entity}) = true",
                'description': f"Credit event on {swap.reference_entity}",
                'is_active': True
            }
            triggers.append(trigger)
        
        elif obligation['obligation_type'] in ['fixed_payment', 'floating_payment', 'premium_payment']:
            # Time-based trigger for regular payments
            trigger = {
                'trigger_type': 'time_based',
                'trigger_condition': f"date >= {obligation.get('due_date', 'TBD')}",
                'description': f"Payment due on {obligation.get('due_date', 'TBD')}",
                'is_active': True
            }
            triggers.append(trigger)
        
        elif obligation['obligation_type'] == 'total_return_payment':
            # Performance-based trigger
            trigger = {
                'trigger_type': 'performance',
                'trigger_condition': f"performance({swap.reference_entity}) != 0",
                'description': f"Performance change in {swap.reference_entity}",
                'is_active': True
            }
            triggers.append(trigger)
        
        # Extract from additional terms if available
        if swap.additional_terms and 'triggers' in swap.additional_terms:
            for trigger_info in swap.additional_terms['triggers']:
                if isinstance(trigger_info, dict):
                    triggers.append(trigger_info)
        
        return triggers
    
    def _determine_instrument_type(self, identifier: str) -> str:
        """Determine the instrument type based on identifier.
        
        Args:
            identifier: Instrument identifier
            
        Returns:
            String representing the instrument type
        """
        identifier = identifier.upper()
        
        # Simple heuristics - in practice, you'd use more sophisticated logic
        if len(identifier) <= 5 and identifier.isalpha():
            return 'equity'
        elif 'INDEX' in identifier or 'IDX' in identifier:
            return 'index'
        elif len(identifier) == 9 and identifier.isalnum():
            return 'bond'  # CUSIP format
        elif len(identifier) == 12 and identifier.isalnum():
            return 'bond'  # ISIN format
        else:
            return 'other'
    
    def _get_payment_frequency_factor(self, frequency: PaymentFrequency) -> int:
        """Get the number of payments per year for a given frequency.
        
        Args:
            frequency: Payment frequency
            
        Returns:
            Number of payments per year
        """
        frequency_map = {
            PaymentFrequency.DAILY: 365,
            PaymentFrequency.WEEKLY: 52,
            PaymentFrequency.MONTHLY: 12,
            PaymentFrequency.QUARTERLY: 4,
            PaymentFrequency.SEMI_ANNUAL: 2,
            PaymentFrequency.ANNUAL: 1,
            PaymentFrequency.MATURITY: 1
        }
        return frequency_map.get(frequency, 4)  # Default to quarterly
    
    def _calculate_next_payment_date(self, start_date: date, frequency: PaymentFrequency) -> date:
        """Calculate the next payment date based on frequency.
        
        Args:
            start_date: Start date of the swap
            frequency: Payment frequency
            
        Returns:
            Next payment date
        """
        from dateutil.relativedelta import relativedelta
        
        if frequency == PaymentFrequency.MONTHLY:
            return start_date + relativedelta(months=1)
        elif frequency == PaymentFrequency.QUARTERLY:
            return start_date + relativedelta(months=3)
        elif frequency == PaymentFrequency.SEMI_ANNUAL:
            return start_date + relativedelta(months=6)
        elif frequency == PaymentFrequency.ANNUAL:
            return start_date + relativedelta(years=1)
        else:
            # Default to quarterly
            return start_date + relativedelta(months=3)
    
    def _process_dataframe(self, df: pd.DataFrame) -> List[SwapContract]:
        """Process swaps data from a pandas DataFrame.
        
        Args:
            df: DataFrame containing swaps data
            
        Returns:
            List of processed SwapContract objects
        """
        swaps = []
        
        # Convert column names to lowercase for case-insensitive matching
        df.columns = df.columns.str.lower()
        
        # Handle missing or differently named columns
        column_mapping = {
            'contract_id': ['contract_id', 'id', 'swap_id', 'contractid', 'dissemination identifier'],
            'counterparty': ['counterparty', 'cp', 'party', 'prime brokerage transaction indicator'],
            'reference_entity': ['reference_entity', 'reference', 'underlying', 'entity', 'underlying asset name', 'underlier id-leg 1'],
            'notional_amount': ['notional_amount', 'notional', 'amount', 'size', 'notional amount-leg 1'],
            'currency': ['currency', 'ccy', 'curr', 'notional currency-leg 1'],
            'effective_date': ['effective_date', 'start_date', 'trade_date'],
            'maturity_date': ['maturity_date', 'end_date', 'expiration date'],
            'swap_type': ['swap_type', 'type', 'product', 'asset class'],
            'payment_frequency': ['payment_frequency', 'freq', 'payment', 'fixed rate payment frequency period-leg 1'],
            'fixed_rate': ['fixed_rate', 'rate', 'coupon', 'fixed rate-leg 1'],
            'floating_rate_index': ['floating_rate_index', 'index', 'floating_index'],
            'floating_rate_spread': ['floating_rate_spread', 'spread', 'margin', 'spread-leg 1']
        }
        
        # Find actual column names in the dataframe
        actual_columns = {}
        for standard_name, possible_names in column_mapping.items():
            for name in possible_names:
                if name in df.columns:
                    actual_columns[standard_name] = name
                    break
        
        # Process each row
        for _, row in df.iterrows():
            try:
                # Extract values using mapped column names
                swap_data = {}
                for std_name, actual_name in actual_columns.items():
                    if actual_name in row and pd.notna(row[actual_name]):
                        swap_data[std_name] = row[actual_name]
                
                # Validate and convert dates
                effective_date_dt = pd.to_datetime(swap_data.get('effective_date'), errors='coerce')
                maturity_date_dt = pd.to_datetime(swap_data.get('maturity_date'), errors='coerce')

                if pd.isna(effective_date_dt) or pd.isna(maturity_date_dt):
                    logger.warning(f"Skipping record with invalid or missing date. Contract ID: {swap_data.get('contract_id', 'N/A')}")
                    continue

                # --- Data Validation and Type Conversion ---
                try:
                    notional = float(swap_data.get('notional_amount', 0))
                except (ValueError, TypeError):
                    logger.warning(f"Could not convert notional_amount '{swap_data.get('notional_amount')}' to float. Skipping row.")
                    continue

                try:
                    fixed_rate = float(swap_data['fixed_rate']) if 'fixed_rate' in swap_data else None
                except (ValueError, TypeError):
                    fixed_rate = None

                try:
                    spread = float(swap_data['floating_rate_spread']) if 'floating_rate_spread' in swap_data and pd.notna(swap_data['floating_rate_spread']) else None
                except (ValueError, TypeError):
                    spread = None

                # Create swap contract
                swap = SwapContract(
                    contract_id=str(swap_data.get('contract_id', '')),
                    counterparty=str(swap_data.get('counterparty', 'UNKNOWN')),
                    reference_entity=str(swap_data.get('reference_entity', 'UNKNOWN')),
                    notional_amount=notional,
                    currency=str(swap_data.get('currency', 'USD')),
                    effective_date=effective_date_dt.date(),
                    maturity_date=maturity_date_dt.date(),
                    swap_type=swap_data.get('swap_type', SwapType.OTHER),
                    payment_frequency=swap_data.get('payment_frequency', PaymentFrequency.QUARTERLY),
                    fixed_rate=fixed_rate,
                    floating_rate_index=swap_data.get('floating_rate_index'),
                    floating_rate_spread=spread,
                )
                
                swaps.append(swap)
                
            except Exception as e:
                # Truncate the error message to prevent RecursionError from rich logging a very long string
                error_message = str(e)
                if len(error_message) > 500:
                    error_message = error_message[:500] + "... (truncated)"
                logger.error(f"Error processing swap record: {error_message}", exc_info=True)
        
        return swaps
    
    def _process_json(self, data: Union[Dict, List]) -> List[SwapContract]:
        """Process swaps data from a JSON structure.
        
        Args:
            data: Dictionary or list containing swaps data
            
        Returns:
            List of processed SwapContract objects
        """
        swaps = []
        
        if isinstance(data, list):
            for item in data:
                try:
                    swap = self._process_swap_item(item)
                    if swap:
                        swaps.append(swap)
                except Exception as e:
                    logger.error(f"Error processing swap item: {str(e)}", exc_info=True)
        elif isinstance(data, dict):
            try:
                swap = self._process_swap_item(data)
                if swap:
                    swaps.append(swap)
            except Exception as e:
                logger.error(f"Error processing swap item: {str(e)}", exc_info=True)
                
        return swaps
    
    def _process_swap_item(self, item: Dict) -> Optional[SwapContract]:
        """Process a single swap item from JSON data.
        
        Args:
            item: Dictionary containing swap data
            
        Returns:
            Processed SwapContract or None if processing failed
        """
        try:
            # Handle nested structures
            if 'data' in item and isinstance(item['data'], dict):
                item = {**item, **item.pop('data')}
                
            # Convert all keys to lowercase for case-insensitive matching
            item = {k.lower(): v for k, v in item.items()}
            
            # Map fields to expected names
            field_mapping = {
                'contract_id': ['contract_id', 'id', 'swap_id', 'contractid'],
                'counterparty': ['counterparty', 'cp', 'party'],
                'reference_entity': ['reference_entity', 'reference', 'underlying', 'entity'],
                'notional_amount': ['notional_amount', 'notional', 'amount', 'size'],
                'currency': ['currency', 'ccy', 'curr'],
                'effective_date': ['effective_date', 'start_date', 'trade_date'],
                'maturity_date': ['maturity_date', 'end_date', 'expiry_date'],
                'swap_type': ['swap_type', 'type', 'product'],
                'payment_frequency': ['payment_frequency', 'freq', 'payment'],
                'fixed_rate': ['fixed_rate', 'rate', 'coupon'],
                'floating_rate_index': ['floating_rate_index', 'index', 'floating_index'],
                'floating_rate_spread': ['floating_rate_spread', 'spread', 'margin'],
                'collateral_terms': ['collateral_terms', 'collateral', 'margin_terms'],
                'additional_terms': ['additional_terms', 'terms', 'misc']
            }
            
            # Extract values using mapped field names
            swap_data = {}
            for field, possible_names in field_mapping.items():
                for name in possible_names:
                    if name in item and item[name] is not None:
                        swap_data[field] = item[name]
                        break
            
            # Skip if required fields are missing
            required_fields = ['contract_id', 'counterparty', 'reference_entity', 
                             'notional_amount', 'effective_date', 'maturity_date']
            if not all(field in swap_data for field in required_fields):
                logger.warning(f"Skipping swap with missing required fields: {item}")
                return None
            
            # Create and return the swap contract
            return SwapContract(
                contract_id=str(swap_data['contract_id']),
                counterparty=swap_data['counterparty'],
                reference_entity=swap_data['reference_entity'],
                notional_amount=float(swap_data['notional_amount']),
                currency=swap_data.get('currency', 'USD'),
                effective_date=swap_data['effective_date'],
                maturity_date=swap_data['maturity_date'],
                swap_type=swap_data.get('swap_type', SwapType.OTHER),
                payment_frequency=swap_data.get('payment_frequency', PaymentFrequency.QUARTERLY),
                fixed_rate=float(swap_data['fixed_rate']) if 'fixed_rate' in swap_data else None,
                floating_rate_index=swap_data.get('floating_rate_index'),
                floating_rate_spread=float(swap_data['floating_rate_spread']) 
                    if 'floating_rate_spread' in swap_data and swap_data['floating_rate_spread'] is not None 
                    else None,
                collateral_terms=swap_data.get('collateral_terms', {}),
                additional_terms=swap_data.get('additional_terms', {})
            )
            
        except Exception as e:
            logger.error(f"Error processing swap item: {str(e)}", exc_info=True)
            return None
    
    def find_swaps_by_reference_entity(self, entity_name: str, use_db: bool = True) -> List[SwapContract]:
        """Find all swaps referencing a specific entity.
        
        Args:
            entity_name: Name of the reference entity to search for
            use_db: Whether to search in the database in addition to in-memory swaps
            
        Returns:
            List of matching SwapContract objects
        """
        # Search in-memory swaps
        in_memory_matches = [
            swap for swap in self._loaded_swaps 
            if entity_name.lower() in swap.reference_entity.lower()
        ]
        
        # Search database if requested
        db_matches = []
        if use_db:
            try:
                db_results = self.db.get_obligations_by_instrument(entity_name)
                db_matches = [SwapContract.from_dict(s) for s in db_results]
            except Exception as e:
                logger.error(f"Error searching swaps in database: {str(e)}")
        
        # Combine and deduplicate results
        all_matches = {swap.contract_id: swap for swap in (in_memory_matches + db_matches)}
        return list(all_matches.values())
    
    def calculate_exposure(self, entity_name: str) -> Dict[str, Any]:
        """Calculate exposure to a specific reference entity.
        
        Args:
            entity_name: Name of the reference entity
            
        Returns:
            Dictionary containing exposure metrics
        """
        entity_swaps = self.find_swaps_by_reference_entity(entity_name)
        if not entity_swaps:
            return {}
        
        total_notional = sum(swap.notional_amount for swap in entity_swaps)
        num_contracts = len(entity_swaps)

        # Aggregate data for analysis
        exposure_by_currency = {}
        exposure_by_counterparty = {}
        exposure_by_type = {}
        maturities = []

        for swap in entity_swaps:
            currency = swap.currency.upper()
            counterparty = swap.counterparty
            swap_type = swap.swap_type.value if hasattr(swap.swap_type, 'value') else str(swap.swap_type)

            exposure_by_currency[currency] = exposure_by_currency.get(currency, 0) + swap.notional_amount
            exposure_by_counterparty[counterparty] = exposure_by_counterparty.get(counterparty, 0) + swap.notional_amount
            exposure_by_type[swap_type] = exposure_by_type.get(swap_type, 0) + swap.notional_amount
            
            if hasattr(swap, 'maturity_date') and swap.maturity_date:
                maturities.append(swap.maturity_date)

        # Find the largest swap
        largest_swap = max(entity_swaps, key=lambda s: s.notional_amount, default=None)

        # Get min/max maturities
        min_maturity = min(maturities) if maturities else None
        max_maturity = max(maturities) if maturities else None

        return {
            'reference_entity': entity_name,
            'total_notional': total_notional,
            'num_swaps': num_contracts,
            'avg_notional': total_notional / num_contracts if num_contracts > 0 else 0,
            'largest_swap': largest_swap.to_dict() if largest_swap else None,
            'counterparties': list(exposure_by_counterparty.keys()),
            'currencies': list(exposure_by_currency.keys()),
            'exposure_by_currency': exposure_by_currency,
            'exposure_by_counterparty': exposure_by_counterparty,
            'exposure_by_type': exposure_by_type,
            'earliest_maturity': min_maturity.isoformat() if min_maturity else None,
            'latest_maturity': max_maturity.isoformat() if max_maturity else None,
            'swap_types': list(exposure_by_type.keys())
        }
    
    def generate_risk_report(self, entity_name: str, include_analysis: bool = False) -> Dict:
        """Generate a risk report for a reference entity.

        Args:
            entity_name: Name of the reference entity
            include_analysis: Whether to include detailed analysis (may be slower)

        Returns:
            Dictionary containing risk metrics and analysis
        """
        exposure = self.calculate_exposure(entity_name)
        if not exposure:
            return {"error": f"No swaps found for reference entity: {entity_name}"}

        today = date.today()
        entity_swaps = self.find_swaps_by_reference_entity(entity_name)

        time_to_maturity = [
            (swap.maturity_date - today).days / 365.25
            for swap in entity_swaps
            if hasattr(swap, 'maturity_date') and swap.maturity_date and (swap.maturity_date - today).days > 0
        ]
        avg_time_to_maturity = sum(time_to_maturity) / len(time_to_maturity) if time_to_maturity else 0

        total_notional = exposure['total_notional']
        counterparty_concentration = max(exposure['exposure_by_counterparty'].values()) / total_notional if total_notional > 0 else 1.0
        currency_concentration = max(exposure['exposure_by_currency'].values()) / total_notional if total_notional > 0 else 1.0

        risk_score = self._calculate_risk_score(
            total_notional=total_notional,
            avg_time_to_maturity=avg_time_to_maturity,
            counterparty_concentration=counterparty_concentration,
            currency_concentration=currency_concentration,
            swap_types=exposure['swap_types']
        )

        if risk_score > 75:
            risk_level = "High"
        elif risk_score > 50:
            risk_level = "Medium"
        else:
            risk_level = "Low"

        report = {
            "reference_entity": entity_name,
            "as_of_date": today.isoformat(),
            "risk_score": risk_score,
            "risk_level": risk_level,
            "total_notional": total_notional,
            "num_swaps": exposure['num_swaps'],
            "avg_time_to_maturity": avg_time_to_maturity,
            "detailed_analysis": {
                "counterparty_concentration": {
                    "value": round(counterparty_concentration, 4),
                    "breakdown": exposure['exposure_by_counterparty']
                },
                "currency_concentration": {
                    "value": round(currency_concentration, 4),
                    "breakdown": exposure['exposure_by_currency']
                },
                "maturity_profile": {
                    "earliest": exposure['earliest_maturity'],
                    "latest": exposure['latest_maturity']
                },
                "swap_type_exposure": exposure['exposure_by_type']
            }
        }

        if include_analysis and self.ollama.is_running() and self.ollama.is_model_available():
            summary_prompt = self._create_risk_summary_prompt(report)
            ai_summary = self.ollama.generate(summary_prompt, max_tokens=256)
            report['ai_summary'] = ai_summary or "Failed to generate AI summary."
        elif include_analysis:
            report['ai_summary'] = "Ollama service not available for AI summary."

        return report
    
    def _calculate_risk_score(
        self, 
        total_notional: float, 
        avg_time_to_maturity: float,
        counterparty_concentration: float,
        currency_concentration: float,
        swap_types: List[str]
    ) -> float:
        """Calculate a composite risk score (0-100)."""
        # Notional risk (0-40 points)
        notional_risk = min(40, (total_notional ** 0.5) / 1000)
        
        # Time to maturity risk (0-20 points)
        time_risk = min(20, avg_time_to_maturity * 2)
        
        # Counterparty concentration risk (0-20 points)
        cp_risk = counterparty_concentration * 20
        
        # Currency concentration risk (0-20 points)
        curr_risk = currency_concentration * 20
        
        # Swap type risk (0-20 points)
        swap_type_risk = len(swap_types) * 5
        
        return min(100, notional_risk + time_risk + cp_risk + curr_risk + swap_type_risk)
    
    def _get_risk_level(self, score: float) -> str:
        """Convert risk score to risk level."""
        if score >= 70:
            return "Very High"
        elif score >= 50:
            return "High"
        elif score >= 30:
            return "Moderate"
        elif score >= 15:
            return "Low"
        else:
            return "Minimal"
    
    def _generate_detailed_analysis(self, swaps: List[SwapContract], exposure: Dict) -> Dict:
        """Generate detailed analysis of swaps."""
        if not swaps:
            return {}
            
        # Calculate metrics by swap type
        metrics_by_type = {}
        for swap in swaps:
            swap_type = swap.swap_type.value if hasattr(swap.swap_type, 'value') else str(swap.swap_type)
            if swap_type not in metrics_by_type:
                metrics_by_type[swap_type] = {
                    'count': 0,
                    'total_notional': 0,
                    'fixed_rate_swaps': 0,
                    'floating_rate_swaps': 0,
                    'avg_notional': 0,
                    'min_maturity': None,
                    'max_maturity': None
                }
            
            metrics = metrics_by_type[swap_type]
            metrics['count'] += 1
            metrics['total_notional'] += swap.notional_amount
            
            if swap.fixed_rate is not None:
                metrics['fixed_rate_swaps'] += 1
            if swap.floating_rate_index is not None:
                metrics['floating_rate_swaps'] += 1
                
            if hasattr(swap, 'maturity_date') and swap.maturity_date:
                if metrics['min_maturity'] is None or swap.maturity_date < metrics['min_maturity']:
                    metrics['min_maturity'] = swap.maturity_date
                if metrics['max_maturity'] is None or swap.maturity_date > metrics['max_maturity']:
                    metrics['max_maturity'] = swap.maturity_date
        
        # Calculate averages
        for metrics in metrics_by_type.values():
            if metrics['count'] > 0:
                metrics['avg_notional'] = metrics['total_notional'] / metrics['count']
        
        # Identify top counterparties
        top_counterparties = sorted(
            exposure['exposure_by_counterparty'].items(), 
            key=lambda x: x[1], 
            reverse=True
        )[:5]  # Top 5
        
        # Identify currency exposures
        currency_exposures = sorted(
            exposure['exposure_by_currency'].items(),
            key=lambda x: x[1],
            reverse=True
        )
        
        return {
            'metrics_by_swap_type': {
                k: {
                    'count': v['count'],
                    'total_notional': v['total_notional'],
                    'avg_notional': v['avg_notional'],
                    'fixed_rate_swaps': v['fixed_rate_swaps'],
                    'floating_rate_swaps': v['floating_rate_swaps'],
                    'min_maturity': v['min_maturity'].isoformat() if v['min_maturity'] else None,
                    'max_maturity': v['max_maturity'].isoformat() if v['max_maturity'] else None
                }
                for k, v in metrics_by_type.items()
            },
            'top_counterparties': [
                {'counterparty': cp, 'notional': amt, 'percentage': (amt / exposure['total_notional']) * 100}
                for cp, amt in top_counterparties
            ],
            'currency_exposures': [
                {'currency': curr, 'notional': amt, 'percentage': (amt / exposure['total_notional']) * 100}
                for curr, amt in currency_exposures
            ]
        }
    
    def analyze_counterparty_risk(self, counterparty: str) -> Dict[str, Any]:
        """Analyze risk exposure to a specific counterparty.
        
        Args:
            counterparty: Name of the counterparty
            
        Returns:
            Dictionary with risk analysis for the counterparty
        """
        # Find all swaps with this counterparty
        swaps = [
            swap for swap in self.swaps 
            if swap.counterparty.lower() == counterparty.lower()
        ]
        
        if not swaps:
            return {"error": f"No swaps found for counterparty: {counterparty}"}
        
        # Calculate exposure metrics
        total_notional = sum(swap.notional_amount for swap in swaps)
        reference_entities = list({swap.reference_entity for swap in swaps})
        
        # Calculate net exposure by reference entity
        net_exposure = {}
        for entity in reference_entities:
            entity_swaps = [s for s in swaps if s.reference_entity == entity]
            net = sum(s.notional_amount * (1 if 'pay' in getattr(s, 'position', '').lower() else -1) 
                     for s in entity_swaps)
            net_exposure[entity] = net
        
        # Calculate concentration risk
        exposure_by_entity = {
            entity: sum(s.notional_amount for s in swaps if s.reference_entity == entity)
            for entity in reference_entities
        }
        max_entity_exposure = max(exposure_by_entity.values()) if exposure_by_entity else 0
        concentration_ratio = max_entity_exposure / total_notional if total_notional > 0 else 0
        
        # Calculate credit exposure metrics
        today = date.today()
        days_to_maturity = [
            (s.maturity_date - today).days 
            for s in swaps 
            if hasattr(s, 'maturity_date') and s.maturity_date
        ]
        avg_days_to_maturity = sum(days_to_maturity) / len(days_to_maturity) if days_to_maturity else 0
        
        return {
            "counterparty": counterparty,
            "total_notional_exposure": total_notional,
            "num_contracts": len(swaps),
            "reference_entities": reference_entities,
            "net_exposure_by_entity": net_exposure,
            "concentration_risk": {
                "max_entity_exposure": max_entity_exposure,
                "concentration_ratio": concentration_ratio,
                "risk_level": "High" if concentration_ratio > 0.5 else "Medium" if concentration_ratio > 0.2 else "Low"
            },
            "maturity_profile": {
                "avg_days_to_maturity": avg_days_to_maturity,
                "earliest_maturity": min((s.maturity_date for s in swaps if hasattr(s, 'maturity') and s.maturity_date), default=None),
                "latest_maturity": max((s.maturity_date for s in swaps if hasattr(s, 'maturity') and s.maturity_date), default=None)
            },
            "swap_types": {
                "credit_default": sum(1 for s in swaps if getattr(s, 'swap_type', '').lower() == 'credit_default'),
                "interest_rate": sum(1 for s in swaps if getattr(s, 'swap_type', '').lower() == 'interest_rate'),
                "total_return": sum(1 for s in swaps if getattr(s, 'swap_type', '').lower() == 'total_return'),
                "other": sum(1 for s in swaps if getattr(s, 'swap_type', '').lower() not in 
                                 ['credit_default', 'interest_rate', 'total_return'])
            },
            "collateral_terms": list({
                json.dumps(s.collateral_terms) 
                for s in swaps 
                if hasattr(s, 'collateral_terms') and s.collateral_terms
            })
        }
    
    def export_to_csv(self, output_path: str, swaps: Optional[List[SwapContract]] = None) -> bool:
        """Export swaps data to a CSV file.
        
        Args:
            output_path: Path to save the CSV file
            swaps: List of swaps to export (defaults to all loaded swaps)
            
        Returns:
            True if export was successful, False otherwise
        """
        try:
            if swaps is None:
                swaps = self.swaps
                
            if not swaps:
                logger.warning("No swaps to export")
                return False
            
            # Convert swaps to list of dictionaries
            data = []
            for swap in swaps:
                swap_dict = {
                    'contract_id': swap.contract_id,
                    'counterparty': swap.counterparty,
                    'reference_entity': swap.reference_entity,
                    'notional_amount': swap.notional_amount,
                    'currency': swap.currency,
                    'effective_date': swap.effective_date.isoformat() if hasattr(swap.effective_date, 'isoformat') else str(swap.effective_date),
                    'maturity_date': swap.maturity_date.isoformat() if hasattr(swap.maturity_date, 'isoformat') else str(swap.maturity_date),
                    'swap_type': swap.swap_type.value if hasattr(swap.swap_type, 'value') else str(swap.swap_type),
                    'payment_frequency': swap.payment_frequency.value if hasattr(swap.payment_frequency, 'value') else str(swap.payment_frequency),
                    'fixed_rate': swap.fixed_rate,
                    'floating_rate_index': swap.floating_rate_index,
                    'floating_rate_spread': swap.floating_rate_spread,
                    'collateral_terms': json.dumps(swap.collateral_terms) if swap.collateral_terms else '',
                    'additional_terms': json.dumps(swap.additional_terms) if swap.additional_terms else ''
                }
                data.append(swap_dict)
            
            # Create DataFrame and export to CSV
            df = pd.DataFrame(data)
            
            # Ensure output directory exists
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Write to CSV
            df.to_csv(output_path, index=False, encoding='utf-8')
            logger.info(f"Successfully exported {len(swaps)} swaps to {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error exporting swaps: {str(e)}")
            return False

    def _calculate_risk_score(
        self,
        total_notional: float,
        avg_time_to_maturity: float,
        counterparty_concentration: float,
        currency_concentration: float,
        swap_types: List[str]
    ) -> float:
        """Calculate a risk score based on several factors."""
        # Define weights for each risk factor
        weights = {
            'notional': 0.30,
            'maturity': 0.20,
            'counterparty': 0.25,
            'currency': 0.15,
            'type': 0.10
        }

        # 1. Notional Amount Score (normalized)
        # Scale logarithmically, score of 50 for $10M notional
        notional_score = min(100, max(0, 10 * (total_notional / 1_000_000) ** 0.5))

        # 2. Maturity Score (longer maturity = higher risk)
        # Score of 50 for 5 years average maturity
        maturity_score = min(100, max(0, (avg_time_to_maturity * 10)))

        # 3. Counterparty Concentration Score
        # Score is directly proportional to concentration
        counterparty_score = min(100, max(0, counterparty_concentration * 100))

        # 4. Currency Concentration Score
        currency_score = min(100, max(0, currency_concentration * 100))

        # 5. Swap Type Risk Score (based on inherent risk of swap types)
        type_risk_weights = {
            SwapType.CREDIT_DEFAULT: 90,
            SwapType.TOTAL_RETURN: 80,
            SwapType.EQUITY: 70,
            SwapType.COMMODITY: 65,
            SwapType.CURRENCY: 50,
            SwapType.INTEREST_RATE: 40,
            SwapType.OTHER: 30
        }
        type_scores = [type_risk_weights.get(SwapType(st), 30) for st in swap_types]
        type_score = sum(type_scores) / len(type_scores) if type_scores else 30

        # Calculate final weighted score
        final_score = (
            notional_score * weights['notional'] +
            maturity_score * weights['maturity'] +
            counterparty_score * weights['counterparty'] +
            currency_score * weights['currency'] +
            type_score * weights['type']
        )

        return round(min(100, max(0, final_score)), 2)

    def _create_risk_summary_prompt(self, report: Dict) -> str:
        """Create a prompt for generating an AI-driven risk summary."""
        details = report['detailed_analysis']
        prompt = f"""
        Analyze the following swap portfolio risk report and provide a concise, high-level executive summary.
        Focus on the overall risk level and the primary contributing factors.

        **Risk Report Summary:**
        - **Reference Entity:** {report['reference_entity']}
        - **Overall Risk Score:** {report['risk_score']:.2f}/100 ({report['risk_level']})
        - **Total Notional Exposure:** ${report['total_notional']:,.2f} across {report['num_swaps']} contracts.
        - **Counterparty Concentration:** {details['counterparty_concentration']['value']:.2%} (The largest counterparty accounts for this percentage of the total notional).
        - **Currency Concentration:** {details['currency_concentration']['value']:.2%} (The largest currency accounts for this percentage of the total notional).
        - **Average Time to Maturity:** {report['avg_time_to_maturity']:.2f} years.

        **Executive Summary:**
        """
        return prompt

    def explain_swap(self, contract_id: str) -> Optional[str]:
        """Generate a plain-language explanation of a swap using Ollama."""
        if not self.ollama.is_running() or not self.ollama.is_model_available():
            logger.error("Ollama is not running or the model is not available.")
            return "Ollama service is not available. Please ensure it is running and the model is downloaded."

        # Fetch swap details from the database view
        all_swaps = self.db.get_swap_obligations_view()
        swap_details_list = [s for s in all_swaps if s['contract_id'] == contract_id]

        if not swap_details_list:
            return f"No swap found with Contract ID: {contract_id}"
        
        # Consolidate swap details
        swap_details = swap_details_list[0]
        obligations = []
        for item in swap_details_list:
            if item.get('obligation_id') and item['obligation_id'] not in [o.get('id') for o in obligations]:
                obligations.append({
                    'id': item['obligation_id'],
                    'type': item['obligation_type'],
                    'amount': item['obligation_amount'],
                    'currency': item['obligation_currency'],
                    'due_date': item['due_date'],
                    'trigger': item.get('trigger_condition')
                })
        
        # Create a detailed prompt for the LLM
        prompt = f"""
        Please provide a clear, plain-language explanation of the following financial swap agreement.
        Focus on the key parties, their obligations, the underlying asset, and what events trigger payments.

        **Swap Details:**
        - **Contract ID:** {swap_details.get('contract_id')}
        - **Swap Type:** {swap_details.get('swap_type', 'N/A')}
        - **Counterparty:** {swap_details.get('counterparty')}
        - **Reference Entity/Security:** {swap_details.get('instrument_identifier', swap_details.get('reference_entity'))}
        - **Notional Amount:** {swap_details.get('currency')} {swap_details.get('notional_amount'):,.2f}
        - **Effective Date:** {swap_details.get('effective_date')}
        - **Maturity Date:** {swap_details.get('maturity_date')}

        **Key Obligations:**
        {self._format_obligations_for_prompt(obligations)}

        **Explanation:**
        """

        try:
            explanation = self.ollama.generate(prompt, max_tokens=512)
            return explanation
        except Exception as e:
            logger.error(f"Error generating swap explanation: {str(e)}")
            return "An error occurred while generating the explanation."

    def _format_obligations_for_prompt(self, obligations: List[Dict]) -> str:
        """Format a list of obligations for inclusion in an LLM prompt."""
        if not obligations:
            return "- No specific obligations listed."
        
        formatted_text = ""
        for ob in obligations:
            formatted_text += f"- **Obligation:** {ob.get('type', 'N/A')}\n"
            formatted_text += f"  - **Amount:** {ob.get('currency')} {ob.get('amount', 0):,.2f}\n"
            formatted_text += f"  - **Due Date:** {ob.get('due_date', 'Contingent')}\n"
            formatted_text += f"  - **Trigger Condition:** {ob.get('trigger', 'N/A')}\n"
        return formatted_text
