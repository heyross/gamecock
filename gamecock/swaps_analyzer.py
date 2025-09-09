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
from .data_structures import SwapContract, SwapType, PaymentFrequency

logger = logging.getLogger(__name__)


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
        self._db_swaps_cache: Optional[List[SwapContract]] = None

    def get_all_swaps_from_db(self) -> List[SwapContract]:
        """Load all swaps from the database, using a cache."""
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
        self._db_swaps_cache = None
        logger.info("Swaps analyzer cache has been cleared.")
    
    def calculate_exposure(self, entity_name: str) -> Dict[str, Any]:
        """Calculate exposure to a specific reference entity.
        
        Args:
            entity_name: Name of the reference entity
            
        Returns:
            Dictionary containing exposure metrics
        """
        swap_dicts = self.db.find_swaps_by_reference_entity(entity_name)
        if not swap_dicts:
            return {}

        entity_swaps = [SwapContract.from_dict(s) for s in swap_dicts]
        
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
            'swaps': entity_swaps, # Return the list of swaps
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
        entity_swaps = exposure['swaps']

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
            swap for swap in self.get_all_swaps_from_db() 
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
                swaps = self.get_all_swaps_from_db()
                
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
