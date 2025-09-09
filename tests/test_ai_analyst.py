import json
from unittest.mock import MagicMock

import pytest

from gamecock.ai_analyst import AIAnalyst


@pytest.fixture()
def analyst():
    db = MagicMock()
    ollama = MagicMock()
    sec = MagicMock()
    return AIAnalyst(db_handler=db, ollama_handler=ollama, sec_handler=sec)


def test_answer_ollama_unavailable_returns_error(analyst):
    analyst.ollama.is_running.return_value = False
    analyst.ollama.is_model_available.return_value = False

    resp = analyst.answer("Analyze risk for ABC")

    assert resp["type"] == "error"
    assert "Ollama service is not available" in resp["message"]


def test_answer_no_entity_found_error(analyst):
    analyst.ollama.is_running.return_value = True
    analyst.ollama.is_model_available.return_value = True
    # Force extractor to return None
    analyst._extract_entity_name = MagicMock(return_value=None)
    resp = analyst.answer("Analyze")
    assert resp["type"] == "error"
    assert "could not identify" in resp["message"].lower()

def test_extract_entity_name_variants(analyst):
    assert analyst._extract_entity_name("Analyze risk for Goldman Sachs?") == "Goldman Sachs"
    assert analyst._extract_entity_name("What is risk of GME") == "GME"
    assert analyst._extract_entity_name("info about TSLA") == "TSLA"
    assert analyst._extract_entity_name("Analyze GME") == "GME"
    assert analyst._extract_entity_name("Analyze") is None

    # Trigger IndexError/fallback branch where preposition is last token
    assert not analyst._extract_entity_name("risk for")


def test_find_entity_match_exact_counterparty(analyst):
    analyst.db.get_all_counterparties.return_value = [{"id": 1, "name": "CP1"}]
    analyst.db.get_all_reference_securities.return_value = []

    result = analyst._find_entity_match("CP1")

    assert result["status"] == "EXACT_MATCH"
    assert result["match"]["type"] == "counterparty"
    assert result["match"]["name"] == "CP1"


def test_find_entity_match_exact_security(analyst):
    analyst.db.get_all_counterparties.return_value = []
    analyst.db.get_all_reference_securities.return_value = [{"id": 2, "identifier": "ABC"}]
    result = analyst._find_entity_match("ABC")
    assert result["status"] == "EXACT_MATCH"
    assert result["match"]["type"] == "security"
    assert result["match"]["name"] == "ABC"


def test_find_entity_match_close(analyst):
    analyst.db.get_all_counterparties.return_value = [{"id": 1, "name": "Counter PTY"}]
    analyst.db.get_all_reference_securities.return_value = []

    result = analyst._find_entity_match("Counterparty")

    assert result["status"] in ("EXACT_MATCH", "CLOSE_MATCH")


def test_find_entity_match_no_match(analyst):
    analyst.db.get_all_counterparties.return_value = []
    analyst.db.get_all_reference_securities.return_value = []
    result = analyst._find_entity_match("ZZZ")
    assert result["status"] == "NO_MATCH"


def test_retrieve_context_data_counterparty_and_none(analyst):
    # No swaps -> None
    analyst.db.get_swaps_by_counterparty_id.return_value = []
    ctx = analyst._retrieve_context_data({"type": "counterparty", "name": "CP1", "id": 1})
    assert ctx is None

    # With swaps -> context dict
    analyst.db.get_swaps_by_counterparty_id.return_value = [
        {"notional_amount": 100.0, "reference_entity": "ABC"},
        {"notional_amount": 50.0, "reference_entity": "XYZ"},
    ]
    ctx = analyst._retrieve_context_data({"type": "counterparty", "name": "CP1", "id": 1})
    assert ctx["entity_name"] == "CP1"
    assert ctx["num_swaps"] == 2
    assert ctx["total_notional_usd"] == "150.00"
    assert set(ctx["involved_securities"]) == {"ABC", "XYZ"}

    # Security entity branch
    analyst.db.get_swaps_by_security_id.return_value = [
        {"notional_amount": 200.0, "reference_entity": "DEF"},
    ]
    ctx_sec = analyst._retrieve_context_data({"type": "security", "name": "ABC", "id": 9})
    assert ctx_sec["entity_type"] == "security"
    assert ctx_sec["total_notional_usd"] == "200.00"


def test_generate_rag_prompt_includes_context(analyst):
    context = {
        "entity_name": "CP1",
        "entity_type": "counterparty",
        "num_swaps": 2,
        "total_notional_usd": "1,000.00",
        "involved_securities": ["ABC", "XYZ"],
        "swaps": [{"a": 1}],
    }
    q = "Summarize risk for CP1"
    prompt = analyst._generate_rag_prompt(q, context)
    assert "CP1" in prompt
    assert "counterparty" in prompt
    assert "1,000.00" in prompt
    assert "ABC" in prompt and "XYZ" in prompt
    assert json.dumps(context["swaps"], indent=2) in prompt
    assert q in prompt


def test_generate_final_analysis_success(analyst):
    analyst.ollama.generate.return_value = "analysis text"
    resp = analyst.generate_final_analysis("prompt")
    assert resp["type"] == "analysis"
    assert resp["message"] == "analysis text"


def test_generate_final_analysis_exception(analyst):
    analyst.ollama.generate.side_effect = Exception("boom")
    resp = analyst.generate_final_analysis("prompt")
    assert resp["type"] == "error"


def test_answer_exact_match_path_generates_analysis(analyst):
    # Ollama available
    analyst.ollama.is_running.return_value = True
    analyst.ollama.is_model_available.return_value = True

    # Force entity extraction
    analyst._extract_entity_name = MagicMock(return_value="CP1")
    # Force match exact
    analyst._find_entity_match = MagicMock(return_value={
        "status": "EXACT_MATCH",
        "match": {"type": "counterparty", "name": "CP1", "id": 1},
    })
    # Provide context
    analyst._retrieve_context_data = MagicMock(return_value={
        "entity_name": "CP1",
        "entity_type": "counterparty",
        "num_swaps": 1,
        "total_notional_usd": "1.00",
        "involved_securities": ["ABC"],
        "swaps": [{"a": 1}],
    })
    # RAG prompt and analysis
    analyst._generate_rag_prompt = MagicMock(return_value="PROMPT")
    analyst.generate_final_analysis = MagicMock(return_value={"type": "analysis", "message": "ok"})

    resp = analyst.answer("Analyze risk for CP1")

    assert resp["type"] == "analysis"
    analyst._generate_rag_prompt.assert_called_once()
    analyst.generate_final_analysis.assert_called_once()


def test_answer_close_match_path_prompts_confirm(analyst):
    analyst.ollama.is_running.return_value = True
    analyst.ollama.is_model_available.return_value = True

    analyst._extract_entity_name = MagicMock(return_value="CPX")
    analyst._find_entity_match = MagicMock(return_value={
        "status": "CLOSE_MATCH",
        "suggestion": {"type": "counterparty", "name": "CP1", "id": 1},
    })

    resp = analyst.answer("Analyze risk for CPX")

    assert resp["type"] == "prompt_confirm_entity"
    assert "Would you like me to analyze" in resp["message"]


def test_answer_no_match_path_prompts_download(analyst):
    analyst.ollama.is_running.return_value = True
    analyst.ollama.is_model_available.return_value = True

    analyst._extract_entity_name = MagicMock(return_value="CPZ")
    analyst._find_entity_match = MagicMock(return_value={"status": "NO_MATCH"})

    resp = analyst.answer("Analyze risk for CPZ")

    assert resp["type"] == "prompt_download"
    assert "Would you like to try downloading" in resp["message"]


def test_answer_exact_but_no_context_returns_error(analyst):
    analyst.ollama.is_running.return_value = True
    analyst.ollama.is_model_available.return_value = True

    analyst._extract_entity_name = MagicMock(return_value="CP1")
    analyst._find_entity_match = MagicMock(return_value={
        "status": "EXACT_MATCH",
        "match": {"type": "counterparty", "name": "CP1", "id": 1},
    })
    analyst._retrieve_context_data = MagicMock(return_value=None)

    resp = analyst.answer("Analyze risk for CP1")

    assert resp["type"] == "error"
    assert "no swaps associated" in resp["message"]
