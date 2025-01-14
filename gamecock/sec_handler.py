"""SEC EDGAR API handler."""
import httpx
from typing import Optional, Dict, Any
from loguru import logger
import time
import os
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
        
        # Initialize rate limiter (9 requests per second max)
        self.rate_limiter = RateLimiter(max_requests=9)
        
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
                else:
                    logger.error(f"SEC API request failed: {response.status_code}")
                    logger.error(f"Response text: {response.text}")
                    return None
                    
        except Exception as e:
            logger.error(f"Error making SEC API request: {str(e)}")
            return None
    
    def get_company_info(self, query: str) -> Optional[CompanyInfo]:
        """Search for company info in SEC EDGAR."""
        try:
            # Clean up query
            query = query.strip().upper()
            logger.info(f"Searching for company: {query}")
            
            # Try ticker/name search first as it's more reliable
            ticker_url = f"{self.base_url}/files/company_tickers.json"
            response = self._make_request(ticker_url)
            if response:
                companies = response.json()
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
                            exchange_response = self._make_request(exchange_url)
                            if exchange_response:
                                exchange_data = exchange_response.json()
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
