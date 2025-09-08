"""AI Analyst powered by a RAG model to query and analyze swaps data."""
import json
from typing import Optional, Dict, Any, List
from loguru import logger
import difflib

from .db_handler import DatabaseHandler
from .ollama_handler import OllamaHandler
from .sec_handler import SECHandler
from .downloader import SECDownloader

class AIAnalyst:
    """Uses a RAG model to provide AI-driven analysis of swaps data."""

    def __init__(self, db_handler: Optional[DatabaseHandler] = None, ollama_handler: Optional[OllamaHandler] = None):
        """Initialize the AI Analyst."""
        self.db = db_handler or DatabaseHandler()
        self.ollama = ollama_handler or OllamaHandler()
        self.sec_handler = SECHandler()
        self.downloader = SECDownloader(db_handler=self.db)

    def answer(self, question: str) -> Dict[str, Any]:
        """Handles a user's question by parsing, retrieving data, and generating a response or a follow-up prompt."""
        logger.info(f"Received question: {question}")

        if not self.ollama.is_running() or not self.ollama.is_model_available():
            return {"type": "error", "message": "Ollama service is not available. Please ensure it is running and the model is downloaded."}

        entity_name = self._extract_entity_name(question)
        if not entity_name:
            return {"type": "error", "message": "I could not identify a company or security name in your question. Please be more specific."}

        entity_match = self._find_entity_match(entity_name)

        if entity_match['status'] == 'EXACT_MATCH':
            entity = entity_match['match']
            context_data = self._retrieve_context_data(entity)
            if not context_data:
                return {"type": "error", "message": f"I found '{entity['name']}' in the database, but there are no swaps associated with it."}
            prompt = self._generate_rag_prompt(question, context_data)
            return self.generate_final_analysis(prompt)
        
        elif entity_match['status'] == 'CLOSE_MATCH':
            suggestion = entity_match['suggestion']
            return {"type": "prompt_confirm_entity", "suggestion": suggestion, "message": f"I didn't find '{entity_name}', but I found '{suggestion['name']}'. Would you like me to analyze that instead?"}
        
        else: # NO_MATCH
            return {"type": "prompt_download", "entity_name": entity_name, "message": f"I have no data for '{entity_name}'. Would you like to try downloading its filings from the SEC?"}

    def generate_final_analysis(self, prompt: str) -> Dict[str, Any]:
        """Generates the final AI analysis and returns it in a structured format."""
        logger.info("Generating final AI analysis...")
        try:
            response = self.ollama.generate(prompt, max_tokens=1024)
            return {"type": "analysis", "message": response or "I was unable to generate a response. Please try again."}
        except Exception as e:
            logger.error(f"Error generating AI response: {e}")
            return {"type": "error", "message": "An error occurred while communicating with the AI model."}

    def _extract_entity_name(self, question: str) -> Optional[str]:
        """Extracts a potential company or security name from the question."""
        words = question.split()
        try:
            for i, word in enumerate(words):
                if word.lower() in ['for', 'of', 'about', 'on']:
                    entity_name = " ".join(words[i+1:])
                    return entity_name.strip('?.,!')
        except IndexError:
            return None
        if len(words) > 1:
            # Fallback for simple questions like "Analyze GME"
            return " ".join(words[1:]).strip('?.,!')
        return None

    def _find_entity_match(self, entity_name: str) -> Dict[str, Any]:
        """Finds an exact or close match for an entity in the database."""
        entity_name_lower = entity_name.lower()
        all_counterparties = self.db.get_all_counterparties()
        all_securities = self.db.get_all_reference_securities()
        
        cp_map = {cp['name'].lower(): {'type': 'counterparty', 'name': cp['name'], 'id': cp['id']} for cp in all_counterparties}
        sec_map = {sec['identifier'].lower(): {'type': 'security', 'name': sec['identifier'], 'id': sec['id']} for sec in all_securities}
        
        if entity_name_lower in cp_map:
            return {'status': 'EXACT_MATCH', 'match': cp_map[entity_name_lower]}
        if entity_name_lower in sec_map:
            return {'status': 'EXACT_MATCH', 'match': sec_map[entity_name_lower]}

        all_names = list(cp_map.keys()) + list(sec_map.keys())
        close_matches = difflib.get_close_matches(entity_name_lower, all_names, n=1, cutoff=0.7)
        
        if close_matches:
            match_name = close_matches[0]
            suggestion = cp_map.get(match_name) or sec_map.get(match_name)
            return {'status': 'CLOSE_MATCH', 'suggestion': suggestion}

        return {'status': 'NO_MATCH'}

    def _retrieve_context_data(self, entity: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Retrieve all relevant data for a given entity."""
        entity_type = entity['type']
        entity_name = entity['name']
        entity_id = entity['id']

        if entity_type == 'counterparty':
            swaps = self.db.get_swaps_by_counterparty_id(entity_id)
        elif entity_type == 'security':
            swaps = self.db.get_swaps_by_security_id(entity_id)
        else:
            return None

        if not swaps:
            return None

        # Consolidate data for the prompt
        total_notional = sum(s['notional_amount'] for s in swaps)
        num_swaps = len(swaps)
        involved_securities = list(set(s['reference_entity'] for s in swaps))

        return {
            'entity_name': entity_name,
            'entity_type': entity_type,
            'num_swaps': num_swaps,
            'total_notional_usd': f"{total_notional:,.2f}",
            'involved_securities': involved_securities,
            'swaps': swaps[:10] # Limit context size for the prompt
        }

    def _generate_rag_prompt(self, question: str, context: Dict[str, Any]) -> str:
        """Generates the final prompt for the LLM, including the retrieved context."""
        prompt = f"""
        You are a financial analyst assistant. Your task is to answer the user's question based *only* on the structured data provided below.
        Do not use any external knowledge. Synthesize the data into a clear, narrative answer.

        **Provided Context Data:**
        - **Analyzed Entity:** {context['entity_name']} ({context['entity_type']})
        - **Total Number of Swaps:** {context['num_swaps']}
        - **Total Notional Value:** ${context['total_notional_usd']} USD
        - **Involved Securities:** {', '.join(context['involved_securities'])}
        - **Sample Swaps Data (up to 10):**
        {json.dumps(context['swaps'], indent=2)}

        **User's Question:**
        {question}

        **Your Analysis:**
        """
        return prompt
