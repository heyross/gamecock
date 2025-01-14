"""SEC EDGAR filing downloader with rate limiting and progress tracking."""
import os
from pathlib import Path
import requests
from datetime import datetime, timedelta
import time
from typing import List, Dict, Optional, Generator
import json
from bs4 import BeautifulSoup
import re
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from loguru import logger
from dotenv import load_dotenv

class SECDownloader:
    """Downloads SEC filings from EDGAR."""
    
    def __init__(self, output_dir: str = None):
        """Initialize the downloader with output directory."""
        if output_dir is None:
            output_dir = str(Path(__file__).parent.parent / "data" / "filings")
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Load environment variables
        load_dotenv()
        
        # SEC rate limiting: 10 requests per second
        self.min_request_interval = 0.1  # seconds
        self.last_request_time = 0
        
        # Set headers with SEC_USER_AGENT from .env
        self.headers = {
            'User-Agent': os.getenv('SEC_USER_AGENT'),
            'Accept-Encoding': 'gzip, deflate',
            'Host': 'www.sec.gov'
        }
        
        if not self.headers['User-Agent']:
            raise ValueError("SEC_USER_AGENT must be set in .env file")
            
    def _wait_for_rate_limit(self):
        """Ensure we don't exceed SEC's rate limit."""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.min_request_interval:
            time.sleep(self.min_request_interval - time_since_last)
        self.last_request_time = time.time()
        
    def _make_request(self, url: str) -> Optional[requests.Response]:
        """Make a request to the SEC API with proper headers and rate limiting."""
        try:
            self._wait_for_rate_limit()
            response = requests.get(url, headers=self.headers, timeout=10)
            
            if response.status_code == 200:
                return response
            else:
                logger.warning(f"Request failed with status {response.status_code} for URL: {url}")
                logger.debug(f"Response content: {response.text[:500]}")  # Log first 500 chars of response
                return None
                
        except requests.RequestException as e:
            logger.error(f"Error making request to {url}: {str(e)}")
            return None
            
    def get_company_filings(
        self,
        cik: str,
        start_date: datetime,
        end_date: datetime,
        filing_types: List[str] = None
    ) -> Generator[Dict, None, None]:
        """Get list of filings for a company within date range."""
        if not cik:
            logger.error("No CIK provided")
            return
            
        try:
            # Normalize CIK by removing leading zeros and then padding to 10 digits
            cik_clean = str(int(''.join(filter(str.isdigit, cik)))).zfill(10)
            logger.info(f"Fetching filings for CIK: {cik_clean}")
            
            # Use the modern EDGAR submissions feed API
            submissions_url = f"https://data.sec.gov/submissions/CIK{cik_clean}.json"
            logger.info(f"Fetching from submissions API: {submissions_url}")
            
            submissions_response = self._make_request(submissions_url)
            if not submissions_response:
                logger.error("Failed to get submissions data")
                return
                
            try:
                submissions_data = submissions_response.json()
                recent_filings = submissions_data.get("filings", {}).get("recent", {})
                
                if not recent_filings:
                    logger.error("No filings data found in submissions response")
                    return
                    
                # Get all the filing metadata arrays
                filing_dates = recent_filings.get("filingDate", [])
                form_types = recent_filings.get("form", [])
                accession_numbers = recent_filings.get("accessionNumber", [])
                file_numbers = recent_filings.get("fileNumber", [])
                primary_docs = recent_filings.get("primaryDocument", [])
                primary_doc_descs = recent_filings.get("primaryDocDescription", [])
                
                # Process each filing
                for idx in range(len(filing_dates)):
                    try:
                        filing_date = datetime.strptime(filing_dates[idx], "%Y-%m-%d")
                        form_type = form_types[idx]
                        accession = accession_numbers[idx]
                        primary_doc = primary_docs[idx] if idx < len(primary_docs) else None
                        
                        # Check if filing is within date range and matches requested types
                        if start_date <= filing_date <= end_date:
                            if not filing_types or form_type in filing_types:
                                # Clean accession number for URL
                                accession_clean = accession.replace("-", "")
                                
                                # Get the filing directory listing
                                filing_url = f"https://www.sec.gov/Archives/edgar/data/{cik_clean}/{accession_clean}/index.json"
                                filing_response = self._make_request(filing_url)
                                
                                if not filing_response:
                                    logger.warning(f"Could not get file list for filing {accession}")
                                    continue
                                    
                                try:
                                    filing_data = filing_response.json()
                                    files = []
                                    
                                    # Get list of files
                                    for file_entry in filing_data.get("directory", {}).get("item", []):
                                        if file_entry["type"] != "dir":
                                            file_info = {
                                                "name": file_entry["name"],
                                                "type": file_entry["type"],
                                                "size": file_entry.get("size", 0)
                                            }
                                            files.append(file_info)
                                            
                                    # Create filing info
                                    filing_info = {
                                        "accession_number": accession,
                                        "filing_date": filing_dates[idx],
                                        "form_type": form_type,
                                        "file_number": file_numbers[idx] if idx < len(file_numbers) else None,
                                        "primary_document": primary_doc,
                                        "primary_doc_description": primary_doc_descs[idx] if idx < len(primary_doc_descs) else None,
                                        "files": files
                                    }
                                    
                                    logger.info(f"Found filing {accession} ({form_type}) from {filing_dates[idx]} with {len(files)} files")
                                    yield filing_info
                                    
                                except Exception as e:
                                    logger.error(f"Error processing file list for filing {accession}: {str(e)}")
                                    continue
                                    
                            else:
                                logger.debug(f"Skipping filing {accession} - form type {form_type} not requested")
                        else:
                            logger.debug(f"Skipping filing {accession} - date {filing_date} outside range")
                            
                    except Exception as e:
                        logger.error(f"Error processing filing metadata at index {idx}: {str(e)}")
                        continue
                        
            except Exception as e:
                logger.error(f"Error processing submissions data: {str(e)}")
                return
                
        except Exception as e:
            logger.error(f"Error fetching filings for CIK {cik}: {str(e)}")
            return
            
    def download_filing(
        self,
        cik: str,
        accession_number: str,
        output_dir: Optional[str] = None
    ) -> List[Path]:
        """Download all files for a specific filing."""
        if output_dir is None:
            output_dir = self.output_dir / cik / accession_number
        else:
            output_dir = Path(output_dir)
            
        output_dir.mkdir(parents=True, exist_ok=True)
        downloaded_files = []
        
        try:
            # Get filing index
            base_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession_number}"
            index_url = f"{base_url}/index.json"
            
            response = self._make_request(index_url)
            filing_data = response.json()
            
            # Download each file in the filing
            for file_entry in filing_data.get("directory", {}).get("item", []):
                if file_entry["type"] != "dir":  # Skip directories
                    file_url = f"{base_url}/{file_entry['name']}"
                    output_file = output_dir / file_entry["name"]
                    
                    # Download file with streaming to handle large files
                    response = self._make_request(file_url)
                    with open(output_file, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                                
                    downloaded_files.append(output_file)
                    
            return downloaded_files
            
        except Exception as e:
            logger.error(f"Error downloading filing {accession_number} for CIK {cik}: {str(e)}")
            raise
            
    def download_company_filings(
        self,
        cik: str,
        start_date: datetime,
        end_date: datetime,
        filing_types: List[str] = None,
        output_dir: Optional[str] = None
    ) -> Dict[str, List[Path]]:
        """Download all filings for a company within date range."""
        if not cik:
            logger.error("No CIK provided")
            return {}
            
        try:
            cik = str(cik).strip().zfill(10)
        except Exception as e:
            logger.error(f"Invalid CIK format: {cik}")
            return {}
            
        if output_dir is None:
            output_dir = self.output_dir / cik
        else:
            output_dir = Path(output_dir)
            
        output_dir.mkdir(parents=True, exist_ok=True)
        downloaded_files = {}
        
        try:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TaskProgressColumn()
            ) as progress:
                # First, get list of filings
                try:
                    filings_task = progress.add_task("Finding filings...", total=None)
                    filings = list(self.get_company_filings(cik, start_date, end_date, filing_types))
                    if not filings:
                        logger.warning(f"No filings found for CIK {cik}")
                        return {}
                    progress.update(filings_task, total=len(filings))
                except Exception as e:
                    logger.error(f"Error getting filing list: {str(e)}")
                    return {}
                
                # Download each filing
                for filing in filings:
                    if not filing.get("accession_number"):
                        logger.warning("Skipping filing with no accession number")
                        continue
                        
                    progress.update(
                        filings_task,
                        description=f"Downloading {filing.get('form_type', 'unknown')} from {filing.get('filing_date', 'unknown date')}"
                    )
                    
                    try:
                        filing_files = self.download_filing(
                            cik,
                            filing["accession_number"],
                            output_dir / filing["accession_number"]
                        )
                        if filing_files:
                            downloaded_files[filing["accession_number"]] = filing_files
                    except Exception as e:
                        logger.error(f"Error downloading filing {filing['accession_number']}: {str(e)}")
                        continue
                        
                    progress.advance(filings_task)
                    
            return downloaded_files
            
        except Exception as e:
            logger.error(f"Error in download process: {str(e)}")
            return {}
            
    def extract_filing_text(self, file_path: Path) -> str:
        """Extract text content from a filing file."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                
            # If it's an HTML file, use BeautifulSoup to extract text
            if file_path.suffix.lower() in ['.htm', '.html']:
                soup = BeautifulSoup(content, 'html.parser')
                # Remove script and style elements
                for script in soup(["script", "style"]):
                    script.decompose()
                text = soup.get_text()
                # Clean up whitespace
                lines = (line.strip() for line in text.splitlines())
                chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
                text = ' '.join(chunk for chunk in chunks if chunk)
                return text
            else:
                return content
                
        except Exception as e:
            logger.error(f"Error extracting text from {file_path}: {str(e)}")
            return ""
            
    def parse_filing_metadata(self, file_path: Path) -> Dict:
        """Extract metadata from a filing file."""
        try:
            text = self.extract_filing_text(file_path)
            metadata = {}
            
            # Common patterns in SEC filings
            patterns = {
                "filing_date": r"FILED AS OF DATE:\s*(\d{8})",
                "document_type": r"CONFORMED SUBMISSION TYPE:\s*(\S+)",
                "company_name": r"COMPANY CONFORMED NAME:\s*(.+?)(?=\n)",
                "cik": r"CENTRAL INDEX KEY:\s*(\d{10})",
                "fiscal_year_end": r"FISCAL YEAR END:\s*(\d{4})",
                "filing_period": r"PERIOD OF REPORT:\s*(\d{8})"
            }
            
            for key, pattern in patterns.items():
                match = re.search(pattern, text)
                if match:
                    metadata[key] = match.group(1)
                    
            return metadata
            
        except Exception as e:
            logger.error(f"Error parsing metadata from {file_path}: {str(e)}")
            return {}
