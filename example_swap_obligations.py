#!/usr/bin/env python3
"""
Example script demonstrating the swap obligations tracking functionality.

This script shows how to:
1. Create swap contracts with underlying instruments and obligations
2. Query the swap obligations view
3. Find obligations by counterparty and instrument
"""

import sys
from pathlib import Path
from datetime import date, datetime

# Add the gamecock package to the path
sys.path.insert(0, str(Path(__file__).parent))

from gamecock.db_handler import DatabaseHandler
from gamecock.swaps_analyzer import SwapsAnalyzer, SwapContract, SwapType, PaymentFrequency

def main():
    """Demonstrate swap obligations functionality."""
    
    # Initialize database handler
    print("Initializing database...")
    db = DatabaseHandler()
    
    # Initialize swaps analyzer
    analyzer = SwapsAnalyzer(db_handler=db)
    
    # Create example swap contracts
    print("\nCreating example swap contracts...")
    
    # Example 1: Credit Default Swap on GME
    gme_cds = SwapContract(
        contract_id="CDS_GME_001",
        counterparty="Goldman Sachs",
        reference_entity="GME",
        notional_amount=1000000,
        effective_date=date(2025, 1, 1),
        maturity_date=date(2026, 1, 1),
        currency="USD",
        swap_type=SwapType.CREDIT_DEFAULT,
        payment_frequency=PaymentFrequency.QUARTERLY,
        additional_terms={
            "premium_rate": 0.015,  # 1.5% premium
            "underlying_instruments": [
                {
                    "instrument_type": "equity",
                    "identifier": "GME",
                    "description": "GameStop Corp. Common Stock",
                    "quantity": 10000
                }
            ]
        }
    )
    
    # Example 2: Interest Rate Swap
    irs = SwapContract(
        contract_id="IRS_USD_001",
        counterparty="JPMorgan Chase",
        reference_entity="USD_LIBOR",
        notional_amount=5000000,
        effective_date=date(2025, 1, 1),
        maturity_date=date(2030, 1, 1),
        currency="USD",
        swap_type=SwapType.INTEREST_RATE,
        payment_frequency=PaymentFrequency.QUARTERLY,
        fixed_rate=3.5,
        floating_rate_index="USD_LIBOR_3M",
        floating_rate_spread=0.25
    )
    
    # Example 3: Total Return Swap on BBBY
    bbby_trs = SwapContract(
        contract_id="TRS_BBBY_001",
        counterparty="Morgan Stanley",
        reference_entity="BBBY",
        notional_amount=2000000,
        effective_date=date(2025, 1, 1),
        maturity_date=date(2025, 12, 31),
        currency="USD",
        swap_type=SwapType.TOTAL_RETURN,
        payment_frequency=PaymentFrequency.MONTHLY,
        additional_terms={
            "underlying_instruments": [
                {
                    "instrument_type": "equity",
                    "identifier": "BBBY",
                    "description": "Bed Bath & Beyond Inc. Common Stock",
                    "quantity": 50000
                }
            ]
        }
    )
    
    # Save swaps to database
    swaps = [gme_cds, irs, bbby_trs]
    saved_count = analyzer._save_swaps_to_db(swaps)
    print(f"Saved {saved_count} swaps to database")
    
    # Query the swap obligations view
    print("\n" + "="*60)
    print("SWAP OBLIGATIONS VIEW")
    print("="*60)
    
    obligations_view = db.get_swap_obligations_view()
    
    for obligation in obligations_view:
        print(f"\nSwap: {obligation['contract_id']} ({obligation['counterparty']})")
        print(f"  Reference Entity: {obligation['reference_entity']}")
        print(f"  Notional: {obligation['currency']} {obligation['notional_amount']:,.2f}")
        print(f"  Maturity: {obligation['maturity_date']}")
        
        if obligation['obligation_id']:
            print(f"  Obligation: {obligation['obligation_type']}")
            print(f"    Amount: {obligation['obligation_currency']} {obligation['obligation_amount']:,.2f}")
            print(f"    Due Date: {obligation['due_date']}")
            print(f"    Status: {obligation['obligation_status']}")
            
            if obligation['trigger_type']:
                print(f"    Trigger: {obligation['trigger_type']} - {obligation['trigger_condition']}")
        
        if obligation['instrument_identifier']:
            print(f"  Underlying: {obligation['instrument_type']} - {obligation['instrument_identifier']}")
    
    # Query obligations by counterparty
    print("\n" + "="*60)
    print("OBLIGATIONS BY COUNTERPARTY: Goldman Sachs")
    print("="*60)
    
    gs_obligations = db.get_obligations_by_counterparty("Goldman Sachs")
    for obligation in gs_obligations:
        print(f"Contract: {obligation['swap_contract_id']}")
        print(f"  Type: {obligation['obligation_type']}")
        print(f"  Amount: {obligation['currency']} {obligation['amount']:,.2f}")
        print(f"  Reference Entity: {obligation['reference_entity']}")
        print(f"  Status: {obligation['status']}")
        print()
    
    # Query obligations by instrument
    print("\n" + "="*60)
    print("OBLIGATIONS BY INSTRUMENT: GME")
    print("="*60)
    
    gme_obligations = db.get_obligations_by_instrument("GME")
    for obligation in gme_obligations:
        print(f"Contract: {obligation['swap_contract_id']}")
        print(f"  Counterparty: {obligation['counterparty']}")
        print(f"  Type: {obligation['obligation_type']}")
        print(f"  Amount: {obligation['currency']} {obligation['amount']:,.2f}")
        print(f"  Status: {obligation['status']}")
        print()
    
    print("\nExample completed successfully!")
    print("\nTo view the database tables directly, you can use:")
    print("  sqlite3 data/swaps.db")
    print("  .tables")
    print("  SELECT * FROM vw_swap_obligations;")

if __name__ == "__main__":
    main()
