"""Processes SEC filings to discover and extract swap data."""
import json
import logging
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Union, Optional

import pandas as pd
from loguru import logger

from .data_structures import SwapContract, SwapType, PaymentFrequency
from .db_handler import DatabaseHandler


class SwapsProcessor:
    """Parses SEC filings to find and extract swap-related data."""

    def __init__(self, db_handler: DatabaseHandler):
        """Initialize the Swaps Processor."""
        self.db = db_handler

    def process_directory(self, directory: Union[str, Path], save_to_db: bool = True):
        """Load all swap files from a directory."""
        directory = Path(directory)
        if not directory.is_dir():
            logger.error(f"Directory not found: {directory}")
            return

        for file_path in directory.glob('**/*'):
            if file_path.is_file() and file_path.suffix.lower() in ['.csv', '.json', '.txt']:
                self.process_filing(file_path, save_to_db=save_to_db)

    def process_filing(self, file_path: Union[str, Path], save_to_db: bool = True) -> List[SwapContract]:
        """Load swaps data from a file."""
        loaded_swaps = []
        try:
            file_path = Path(file_path)
            if not file_path.exists():
                logger.error(f"File not found: {file_path}")
                return loaded_swaps

            logger.info(f"Processing swaps from {file_path}")

            suffix = file_path.suffix.lower()
            if suffix == '.csv':
                # Standard CSV
                df = pd.read_csv(file_path)
                loaded_swaps = self._process_dataframe(df)
            elif suffix == '.xlsx':
                # Excel support
                df = pd.read_excel(file_path)
                loaded_swaps = self._process_dataframe(df)
            elif suffix == '.txt':
                # Robust TXT strategy: try several delimiters and fallback to fixed-width
                df = None
                # 1) Try pandas engine guessing
                try:
                    df = pd.read_csv(file_path, sep=None, engine='python')
                except Exception:
                    pass
                # 2) Try tab
                if df is None:
                    try:
                        df = pd.read_csv(file_path, sep='\t')
                    except Exception:
                        pass
                # 3) Try pipe
                if df is None:
                    try:
                        df = pd.read_csv(file_path, sep='|')
                    except Exception:
                        pass
                # 4) Try comma with quoting
                if df is None:
                    try:
                        df = pd.read_csv(file_path, sep=',', engine='python')
                    except Exception:
                        pass
                # 5) Fixed width fallback
                if df is None:
                    try:
                        df = pd.read_fwf(file_path)
                    except Exception:
                        pass
                if df is not None and not df.empty:
                    loaded_swaps = self._process_dataframe(df)
                else:
                    logger.warning(f"TXT file could not be parsed as a table: {file_path}")
            elif file_path.suffix.lower() == '.json':
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    loaded_swaps = self._process_json(data)
            else:
                logger.warning(f"Unsupported file format for swaps: {file_path.suffix}")

            if save_to_db and loaded_swaps:
                self._save_swaps_to_db(loaded_swaps)

        except Exception as e:
            logger.error(f"Error loading swaps data from {file_path}: {str(e)}", exc_info=True)

        return loaded_swaps

    def _save_swaps_to_db(self, swaps: List[SwapContract]) -> int:
        """Save a list of swaps to the database, ensuring entities are created."""
        saved_count = 0
        for swap in swaps:
            try:
                # Ensure counterparty and reference_entity exist before saving swap
                self.db.get_or_create_counterparty(swap.counterparty)
                self.db.get_or_create_security(swap.reference_entity)

                swap_dict = swap.to_dict()
                saved_swap = self.db.save_swap(swap_dict)
                
                if saved_swap:
                    saved_count += 1
            except Exception as e:
                logger.error(f"Error saving swap {swap.contract_id} to database: {str(e)}")

        if saved_count > 0:
            logger.info(f"Successfully saved {saved_count} swaps to the database.")

        return saved_count

    def _process_dataframe(self, df: pd.DataFrame) -> List[SwapContract]:
        """Process swaps data from a pandas DataFrame."""
        swaps = []
        skipped_invalid_date = 0
        df.columns = df.columns.str.lower()
        
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
        
        actual_columns = {}
        for standard_name, possible_names in column_mapping.items():
            for name in possible_names:
                if name in df.columns:
                    actual_columns[standard_name] = name
                    break
        
        for _, row in df.iterrows():
            try:
                swap_data = {}
                for std_name, actual_name in actual_columns.items():
                    if actual_name in row and pd.notna(row[actual_name]):
                        swap_data[std_name] = row[actual_name]
                
                effective_date_dt = pd.to_datetime(swap_data.get('effective_date'), errors='coerce')
                maturity_date_dt = pd.to_datetime(swap_data.get('maturity_date'), errors='coerce')

                if pd.isna(effective_date_dt) or pd.isna(maturity_date_dt):
                    skipped_invalid_date += 1
                    continue

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
                error_message = str(e)
                if len(error_message) > 500:
                    error_message = error_message[:500] + "... (truncated)"
                logger.error(f"Error processing swap record: {error_message}", exc_info=True)
        
        if skipped_invalid_date:
            logger.warning(f"Skipped {skipped_invalid_date} record(s) with invalid or missing date.")
        if swaps:
            logger.info(f"Parsed {len(swaps)} swap record(s) from dataframe.")
        
        return swaps

    def _process_json(self, data: Union[Dict, List]) -> List[SwapContract]:
        """Process swaps data from a JSON structure."""
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
        """Process a single swap item from JSON data."""
        try:
            if 'data' in item and isinstance(item['data'], dict):
                item = {**item, **item.pop('data')}
                
            item = {k.lower(): v for k, v in item.items()}
            
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
            
            swap_data = {}
            for field, possible_names in field_mapping.items():
                for name in possible_names:
                    if name in item and item[name] is not None:
                        swap_data[field] = item[name]
                        break
            
            required_fields = ['contract_id', 'counterparty', 'reference_entity', 
                             'notional_amount', 'effective_date', 'maturity_date']
            if not all(field in swap_data for field in required_fields):
                logger.warning(f"Skipping swap with missing required fields: {item}")
                return None
            
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
                collateral_terms=swap_data.get('coll_terms', {}),
                additional_terms=swap_data.get('add_terms', {})
            )
            
        except Exception as e:
            logger.error(f"Error processing swap item: {str(e)}", exc_info=True)
            return None

