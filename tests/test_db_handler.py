from datetime import date
from unittest.mock import MagicMock

import pytest

from gamecock.db_handler import DatabaseHandler


@pytest.fixture()
def handler():
    # Use in-memory SQLite for isolation
    return DatabaseHandler(db_url="sqlite:///:memory:")


def make_swap(contract_id="c1", counterparty="CP1", reference_entity="ABC", notional=100.0):
    return {
        "contract_id": contract_id,
        "counterparty": counterparty,
        "reference_entity": reference_entity,
        "notional_amount": notional,
        "currency": "USD",
        "effective_date": "2023-01-01",
        "maturity_date": "2025-01-01",
        "swap_type": "CDS",
        "payment_frequency": "Quarterly",
    }


def test_get_or_create_entities(handler):
    handler.get_or_create_counterparty("CP1")
    # Verify via list API to avoid DetachedInstance access
    cps = handler.get_all_counterparties()
    ids = [c["id"] for c in cps if c["name"].lower() == "cp1"]
    assert len(ids) == 1

    handler.get_or_create_security("ABC")
    secs = handler.get_all_reference_securities()
    sids = [s["id"] for s in secs if s["identifier"].lower() == "abc"]
    assert len(sids) == 1


def test_save_get_find_and_delete_swap(handler):
    saved = handler.save_swap(make_swap())
    assert saved["contract_id"] == "c1"

    fetched = handler.get_swap("c1")
    assert fetched["reference_entity"] == "ABC"

    found = handler.find_swaps_by_reference_entity("AB")
    assert any(s["contract_id"] == "c1" for s in found)

    # Update path
    saved2 = handler.save_swap({**make_swap(), "notional_amount": 200.0})
    assert saved2["notional_amount"] == 200.0

    # Delete
    assert handler.delete_swap("c1") is True
    assert handler.get_swap("c1") is None


def test_add_obligation_and_trigger_and_view(handler):
    handler.save_swap(make_swap(contract_id="c2"))
    swap = handler.get_swap("c2")
    obl = handler.add_obligation(swap_id=swap["id"], obligation_data={
        "obligation_type": "Payment",
        "amount": 10.0,
        "currency": "USD",
        "status": "pending",
    })
    assert obl["id"] is not None

    trig = handler.add_obligation_trigger(obl["id"], {
        "trigger_type": "Threshold",
        "trigger_condition": "Price < 10",
        "description": "Auto-pay",
        "is_active": True,
    })
    assert trig["id"] is not None

    # View should include obligation row
    view_rows = handler.get_swap_obligations_view(swap_id=swap["id"])
    assert any(r["swap_id"] == swap["id"] for r in view_rows)


def test_save_analysis_and_get_with_analysis(handler):
    handler.save_swap(make_swap(contract_id="c3"))
    swap = handler.get_swap("c3")
    analysis = handler.save_analysis(swap["id"], {
        "analysis_text": "Text",
        "risk_score": 42.0,
        "key_risks": {"x": 1},
    })
    assert analysis["risk_score"] == 42.0

    combined = handler.get_swap_with_analysis("c3")
    assert combined["analysis"]["analysis_text"] == "Text"


def test_add_underlying_and_instrument_queries(handler):
    handler.save_swap(make_swap(contract_id="c4", counterparty="CPX", reference_entity="XYZ"))
    swap = handler.get_swap("c4")

    inst = handler.add_underlying_instrument(swap["id"], {
        "instrument_type": "Bond",
        "identifier": "XYZ",
        "description": "Corp bond",
        "quantity": 100,
        "notional_amount": 50.0,
        "currency": "USD",
    })
    assert inst["instrument_type"] == "Bond"

    # Query by counterparty name
    obls_by_cp = handler.get_obligations_by_counterparty("CPX")
    assert obls_by_cp == []  # no obligations yet

    # Query by instrument identifier (should at least not fail and return empty obligations without obligations)
    obls_by_inst = handler.get_obligations_by_instrument("XYZ")
    assert obls_by_inst == []

    # add obligation and verify counterparty/instrument queries include enriched fields
    handler.add_obligation(swap_id=swap["id"], obligation_data={
        "obligation_type": "Payment",
        "amount": 5.0,
        "currency": "USD",
        "status": "due",
    })
    obls_by_cp = handler.get_obligations_by_counterparty("CPX")
    assert any(o["swap_contract_id"] == "c4" for o in obls_by_cp)

    obls_by_inst = handler.get_obligations_by_instrument("XYZ")
    assert any(o["instrument_identifier"] == "XYZ" for o in obls_by_inst)


def test_get_all_lists_and_by_ids(handler):
    handler.save_swap(make_swap(contract_id="c5", counterparty="CPA", reference_entity="AAA"))
    handler.save_swap(make_swap(contract_id="c6", counterparty="CPB", reference_entity="BBB"))

    cps = handler.get_all_counterparties()
    secs = handler.get_all_reference_securities()
    assert any(c["name"] == "CPA" for c in cps)

    # by IDs
    cp_id = next(c["id"] for c in cps if c["name"] == "CPB")
    handler.get_or_create_security("AAA")
    sec_id = next(s["id"] for s in handler.get_all_reference_securities() if s["identifier"] == "AAA")

    by_cp = handler.get_swaps_by_counterparty_id(cp_id)
    by_sec = handler.get_swaps_by_security_id(sec_id)

    assert any(s["counterparty"] == "CPB" for s in by_cp)
    # Underlying instrument is required to link by security; add it
    s = handler.get_swap("c5")
    handler.add_underlying_instrument(s["id"], {
        "instrument_type": "Equity",
        "identifier": "AAA",
    })
    by_sec = handler.get_swaps_by_security_id(sec_id)
    assert any(ss["reference_entity"] == "AAA" for ss in by_sec)
