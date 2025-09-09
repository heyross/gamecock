"""SEC EDGAR API handler."""
import httpx
from typing import Optional, Dict, Any
from loguru import logger
import time
import os
from pathlib import Path
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
from .data_structures import CompanyInfo, EntityIdentifiers
from .rate_limiter import RateLimiter

# Load environment variables
load_dotenv()

class SECHandler:
    """Handler for SEC EDGAR API."""
    
    def __init__(self):
        """Initialize SEC handler."""
        self.base_url = "https://www.sec.gov"
        
        # Get credentials from environment variables
        name = os.getenv('SEC_API_NAME', 'Unknown')
        email = os.getenv('SEC_API_EMAIL', 'unknown@example.com')
        
        # Set up headers according to SEC guidelines
        self.headers = {
            "User-Agent": f"{name} {email}",
            "Accept-Encoding": "gzip, deflate",
            "Host": "www.sec.gov"
        }
        
        # Initialize rate limiter (be conservative to avoid 429s)
        self.rate_limiter = RateLimiter(max_requests=2)
        
    def _make_request(self, url: str) -> Optional[httpx.Response]:
        """Make a rate-limited request to SEC API."""
        try:
            # Acquire token from rate limiter
            self.rate_limiter.acquire()
            
            logger.debug(f"Making request to: {url}")
            # Make request
            with httpx.Client(timeout=30.0) as client:
                response = client.get(url, headers=self.headers)
                logger.debug(f"Response status: {response.status_code}")
                
                if response.status_code == 200:
                    return response
                elif response.status_code == 429:
                    # Respect Retry-After if present, otherwise log and return
                    retry_after = response.headers.get("Retry-After")
                    if retry_after:
                        logger.warning(f"Rate limited (429). Retry-After: {retry_after} seconds")
                    else:
                        logger.error("SEC API request rate-limited (429). Consider waiting ~10 minutes.")
                    return None
                else:
                    logger.error(f"SEC API request failed: {response.status_code}")
                    # Avoid dumping full HTML pages to logs repeatedly
                    logger.error(f"Response text: {response.text[:500]}..." if len(response.text) > 500 else f"Response text: {response.text}")
                    return None
            
        except Exception as e:
            logger.error(f"Error making SEC API request: {str(e)}")
            return None

    def _cache_dir(self) -> Path:
        p = Path(__file__).parent / "data" / "cache"
        p.mkdir(parents=True, exist_ok=True)
        return p

    def _load_cached_json(self, name: str, max_age_hours: int = 24) -> Optional[Dict[str, Any]]:
        path = self._cache_dir() / name
        try:
            if path.exists():
                mtime = datetime.fromtimestamp(path.stat().st_mtime)
                if datetime.now() - mtime < timedelta(hours=max_age_hours):
                    with open(path, "r", encoding="utf-8") as f:
                        return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to read cache {path}: {e}")
        return None

    def _save_cached_json(self, name: str, data: Dict[str, Any]) -> None:
        path = self._cache_dir() / name
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f)
        except Exception as e:
            logger.warning(f"Failed to write cache {path}: {e}")
    
    def get_company_info(self, query: str) -> Optional[CompanyInfo]:
        """Search for company info in SEC EDGAR."""
        try:
            # Clean up query
            query = query.strip().upper()
            logger.info(f"Searching for company: {query}")
            
            # Try ticker/name search first as it's more reliable
            ticker_url = f"{self.base_url}/files/company_tickers.json"
            cached = self._load_cached_json("company_tickers.json", max_age_hours=24)
            if cached is not None:
                companies = cached
            else:
                response = self._make_request(ticker_url)
                if response:
                    companies = response.json()
                    self._save_cached_json("company_tickers.json", companies)
                else:
                    logger.warning("Falling back to cached tickers due to request failure")
                    companies = self._load_cached_json("company_tickers.json", max_age_hours=168) or {}
            # Search through companies
            for company_data in companies.values():
                company_name = company_data.get('title', '').upper()
                company_ticker = company_data.get('ticker', '').upper()
                
                if query == company_ticker or query in company_name:
                    logger.info(f"Found match: {company_name} ({company_ticker})")
                    cik = str(company_data['cik_str']).zfill(10)
                    name = company_data['title']
                    ticker = company_data['ticker']
                    
                    # Create identifiers with default exchange
                    primary = EntityIdentifiers(
                        name=name,
                        cik=cik,
                        description=f"Trading as {ticker}",
                        tickers=[{
                            "symbol": ticker,
                            "exchange": "NYSE"  # Default exchange
                        }]
                    )
                    
                    # Try to get exchange info
                    try:
                        exchange_url = f"{self.base_url}/files/company_tickers_exchange.json"
                        exch_cached = self._load_cached_json("company_tickers_exchange.json", max_age_hours=24)
                        if exch_cached is not None:
                            exchange_data = exch_cached
                        else:
                            exchange_response = self._make_request(exchange_url)
                            if exchange_response:
                                exchange_data = exchange_response.json()
                                self._save_cached_json("company_tickers_exchange.json", exchange_data)
                            else:
                                exchange_data = self._load_cached_json("company_tickers_exchange.json", max_age_hours=168) or {}
                        # The data is in a list under the 'data' key
                        for row in exchange_data.get('data', []):
                            if row and len(row) >= 3:  # Make sure we have enough elements
                                exchange_cik = str(row[2]).zfill(10)  # CIK is the 3rd element
                                if exchange_cik == cik:
                                    exchange = row[3] if len(row) > 3 else "NYSE"  # Exchange is 4th element
                                    primary.tickers[0]['exchange'] = exchange
                                    break
                    except Exception as e:
                        logger.warning(f"Could not get exchange info: {str(e)}")
                        # Continue with default exchange
                    
                    return CompanyInfo(
                        name=name,
                        primary_identifiers=primary,
                        related_entities=[]
                    )
                    
            logger.info("No matches found in company tickers")
            return None
                        
        except Exception as e:
            logger.error(f"Error searching SEC EDGAR: {str(e)}")
            logger.error(f"Query was: {query}")
            import traceback
            logger.error(traceback.format_exc())
            return None
