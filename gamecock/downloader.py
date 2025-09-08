"""SEC EDGAR filing downloader with rate limiting and progress tracking."""
import json
import os
import sqlite3
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Union

import requests
from dotenv import load_dotenv
from loguru import logger
from rich.console import Console
from rich.progress import Progress, BarColumn, TaskProgressColumn, TextColumn

from .db_handler import DatabaseHandler
from .swaps_analyzer import SwapsAnalyzer

logger.remove()  # Remove default handler
logger.add(sys.stderr, level="DEBUG")  # Add handler with DEBUG level


class SECDownloader:
    """Downloads SEC filings from EDGAR."""
    
    def __init__(self, output_dir: Union[str, Path] = None, db_handler: Optional[DatabaseHandler] = None, swaps_analyzer: Optional[SwapsAnalyzer] = None):
        """Initialize the downloader."""
        # Load environment variables
        load_dotenv()
        
        # Set headers with SEC_USER_AGENT from .env
        self.headers = {
            'User-Agent': os.getenv('SEC_USER_AGENT'),
            'Accept-Encoding': 'gzip, deflate'
        }
        
        if not self.headers['User-Agent']:
            raise ValueError("SEC_USER_AGENT not set in .env file")
        
        # Set output directory
        self.output_dir = Path(output_dir) if output_dir else Path.cwd() / 'downloads'
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize database and analyzer
        self.db = db_handler or DatabaseHandler()
        self.swaps_analyzer = swaps_analyzer or SwapsAnalyzer(db_handler=self.db)
        
        # Rate limiting
        self.min_request_interval = 0.1  # seconds between requests
        self.last_request_time = 0
        
        # Initialize session
        self.session = requests.Session()
        
    def _wait_for_rate_limit(self):
        """Ensure we don't exceed SEC's rate limit."""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.min_request_interval:
            time.sleep(self.min_request_interval - time_since_last)
        self.last_request_time = time.time()
        
    def _make_request(self, url: str) -> Optional[requests.Response]:
        """Make a request to SEC with proper headers and rate limiting."""
        try:
            logger.debug(f"Making request to: {url}")
            logger.debug(f"Using headers: {self.headers}")
            
            # Wait for rate limit
            self._wait_for_rate_limit()
            
            # Make the request
            response = self.session.get(url, headers=self.headers)
            logger.debug(f"Response status code: {response.status_code}")
            
            if response.status_code == 200:
                return response
            else:
                logger.error(f"Request failed with status {response.status_code}: {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"Request error: {str(e)}")
            return None
            
    def get_company_filings(
        self,
        cik: str,
        start_date: datetime,
        end_date: datetime,
        filing_types: List[str] = None
    ) -> List[Dict]:
        """Get list of filings for a company within date range."""
        try:
            # Format CIK to 10 digits with leading zeros
            cik_formatted = str(cik).zfill(10)
            logger.info(f"Getting filings for CIK: {cik_formatted}")
            logger.info(f"Date range: {start_date.date()} to {end_date.date()}")
            if filing_types:
                logger.info(f"Filtering for form types: {filing_types}")
            
            # Build URL for company submissions
            url = f"https://data.sec.gov/submissions/CIK{cik_formatted}.json"
            logger.info(f"Fetching submissions from: {url}")
            
            # Get company submissions
            response = self._make_request(url)
            if not response:
                logger.error("Failed to get response from SEC API")
                return []
                
            data = response.json()
            logger.debug(f"Received company data: {json.dumps(data.get('name', ''), indent=2)}")
            
            # Get recent filings
            filings = data.get('filings', {}).get('recent', {})
            if not filings:
                logger.warning("No recent filings found in response")
                return []
                
            # Get the indices of filings within our date range
            dates = filings.get('filingDate', [])
            if not dates:
                logger.warning("No filing dates found in response")
                return []
                
            logger.info(f"Found {len(dates)} total filings")
            
            # Store all matching filings
            matching_filings = []
            
            for i, date_str in enumerate(dates):
                try:
                    filing_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                    if start_date.date() <= filing_date <= end_date.date():
                        # Get filing info regardless of type
                        accession_number = filings.get('accessionNumber', [])[i]
                        if not accession_number:
                            logger.warning(f"Missing accession number for filing at index {i}")
                            continue
                            
                        # Clean up accession number
                        accession_number = accession_number.replace('-', '')
                        form_type = filings.get('form', [])[i]
                        logger.info(f"Processing filing {accession_number} ({form_type})")
                        
                        # Check if we want this filing type
                        if filing_types and form_type not in filing_types:
                            logger.debug(f"Skipping filing type: {form_type}")
                            continue
                            
                        try:
                            files = list(self.get_filing_files(cik_formatted, accession_number))
                            logger.info(f"Found {len(files)} files for filing {accession_number}")
                        except Exception as e:
                            logger.error(f"Error getting files for filing {accession_number}: {str(e)}")
                            files = []
                        
                        filing_info = {
                            "accession_number": accession_number,
                            "filing_date": date_str,
                            "form_type": form_type,
                            "is_xbrl": filings.get('isXBRL', [])[i],
                            "is_inline_xbrl": filings.get('isInlineXBRL', [])[i],
                            "primary_document": filings.get('primaryDocument', [])[i],
                            "file_number": filings.get('fileNumber', [])[i],
                            "film_number": filings.get('filmNumber', [])[i],
                            "size": filings.get('size', [])[i],
                            "files": files
                        }
                        
                        logger.debug(f"Filing details: {json.dumps(filing_info, indent=2)}")
                        matching_filings.append(filing_info)
                        
                except ValueError:
                    logger.warning(f"Invalid date format: {date_str}")
                    continue
                    
            logger.info(f"Found {len(matching_filings)} filings within date range")
            return matching_filings
                    
        except Exception as e:
            logger.error(f"Error getting company filings: {str(e)}")
            return []

    def get_filing_files(self, cik: str, accession_number: str) -> List[Dict]:
        """Get list of files for a filing."""
        try:
            # Format CIK to 10 digits with leading zeros
            cik_formatted = str(cik).zfill(10)
            
            # Build URL for the directory listing
            url = f"https://www.sec.gov/Archives/edgar/data/{cik_formatted}/{accession_number}/index.json"
            logger.info(f"Fetching file list from: {url}")
            
            # Get directory listing
            response = self._make_request(url)
            if not response:
                logger.error("Failed to get response from SEC API")
                return []
                
            try:
                data = response.json()
                logger.debug(f"Received directory data: {json.dumps(data, indent=2)}")
                
                # Get directory information
                directory = data.get('directory', {})
                if not directory:
                    logger.warning("No directory information found")
                    return []
                    
                # Get entries
                entries = directory.get('item', [])
                if not entries:
                    logger.warning("No entries found in directory")
                    return []
                    
                # Process each entry
                files = []
                for entry in entries:
                    try:
                        # Skip directories
                        if entry.get('type') == 'dir':
                            continue
                            
                        # Get file information
                        file_info = {
                            'name': entry.get('name'),
                            'type': entry.get('type'),
                            'size': entry.get('size'),
                            'last_modified': entry.get('last_modified')
                        }
                        
                        # Skip entries without names
                        if not file_info['name']:
                            continue
                            
                        files.append(file_info)
                        
                    except Exception as e:
                        logger.error(f"Error processing entry {entry}: {str(e)}")
                        continue
                        
                return files
                
            except json.JSONDecodeError:
                logger.error(f"Invalid JSON in response from {url}")
                return []
                    
        except Exception as e:
            logger.error(f"Error getting filing files: {str(e)}")
            return []

    def download_filing(
        self,
        cik: str,
        accession_number: str,
        output_dir: Optional[str] = None,
        files: Optional[List[Dict]] = None
    ) -> Dict[str, Path]:
        """Download all files for a specific filing."""
        try:
            # Format CIK
            cik_formatted = str(cik).strip()
            logger.info(f"Starting download for filing {accession_number} (CIK: {cik_formatted})")
            
            # Set output directory
            if output_dir is None:
                output_dir = self.output_dir / cik_formatted / accession_number
                
            output_dir = Path(output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"Using output directory: {output_dir}")
            
            downloaded_files = {}
            
            # If no files provided, get the list
            if not files:
                try:
                    logger.info("No file list provided, fetching file list...")
                    files = list(self.get_filing_files(cik_formatted, accession_number))
                    logger.info(f"Found {len(files)} files to download")
                except Exception as e:
                    logger.error(f"Error getting files for filing {accession_number}: {str(e)}")
                    return downloaded_files
            
            # Download each file
            total_files = len(files)
            logger.info(f"Starting download of {total_files} files")
            
            with Progress() as progress:
                download_task = progress.add_task("Downloading...", total=total_files)
                
                for file_info in files:
                    try:
                        if 'name' not in file_info:
                            logger.warning("File info missing name field, skipping")
                            continue
                            
                        file_name = file_info['name']
                        file_path = output_dir / file_name
                        logger.info(f"Processing file: {file_name}")
                        
                        # Skip if file already exists and has content
                        if file_path.exists() and file_path.stat().st_size > 0:
                            logger.info(f"File already exists and has content: {file_path}")
                            downloaded_files[file_name] = file_path
                            progress.advance(download_task)
                            continue
                        
                        # Build URL for the file
                        url = f"https://www.sec.gov/Archives/edgar/data/{cik_formatted}/{accession_number}/{file_name}"
                        logger.debug(f"Downloading from URL: {url}")
                        
                        # Download the file
                        response = self._make_request(url)
                        if response and response.content:
                            try:
                                # Ensure the directory exists
                                file_path.parent.mkdir(parents=True, exist_ok=True)
                                
                                # Write the file in binary mode
                                with open(str(file_path), 'wb') as f:
                                    f.write(response.content)
                                    f.flush()
                                    os.fsync(f.fileno())  # Force write to disk
                                
                                # Verify the file was written
                                if file_path.exists() and file_path.stat().st_size > 0:
                                    downloaded_files[file_name] = file_path
                                    logger.info(f"Successfully downloaded: {file_path} ({file_path.stat().st_size} bytes)")
                                else:
                                    logger.error(f"File not written correctly: {file_path}")
                            except Exception as e:
                                logger.error(f"Error writing file {file_path}: {str(e)}")
                                if file_path.exists():
                                    try:
                                        file_path.unlink()
                                        logger.info(f"Cleaned up failed download: {file_path}")
                                    except Exception as cleanup_err:
                                        logger.error(f"Error cleaning up file: {str(cleanup_err)}")
                        else:
                            logger.error(f"Failed to download {url} or response was empty")
                            
                        progress.advance(download_task)
                            
                    except Exception as e:
                        logger.error(f"Error downloading file {file_info.get('name', 'unknown')}: {str(e)}")
                        continue
            
            logger.info(f"Download complete. Successfully downloaded {len(downloaded_files)} out of {total_files} files")
            return downloaded_files
            
        except Exception as e:
            logger.error(f"Error downloading filing {accession_number}: {str(e)}")
            return {}
            
    def download_company_filings(
        self,
        cik: str,
        start_date: datetime,
        end_date: datetime,
        filing_types: List[str] = None
    ) -> Dict[str, List[Path]]:
        """Download all filings for a company within date range."""
        if not cik:
            logger.error("No CIK provided")
            return {}
            
        logger.info(f"Starting download for CIK: {cik}")
        logger.info(f"Date range: {start_date.date()} to {end_date.date()}")
        if filing_types:
            logger.info(f"Filtering for form types: {filing_types}")
        
        # Format CIK (keep original format for directory structure)
        cik_formatted = str(cik).strip()
        
        downloaded_files = {}
        
        try:
            with Progress(
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                console=Console(force_terminal=True)
            ) as progress:
                # First, get list of filings
                find_task = progress.add_task("Finding filings...", total=None)
                filings = self.get_company_filings(cik, start_date, end_date, filing_types)
                if not filings:
                    logger.warning(f"No filings found for CIK {cik}")
                    return {}
                progress.update(find_task, total=1, completed=1)
                logger.info(f"Found {len(filings)} filings to download")
                
                # Create a new task for downloads
                download_task = progress.add_task(
                    f"Downloading {len(filings)} filings...",
                    total=len(filings)
                )
                
                # Download each filing
                for filing in filings:
                    filing_dir = None
                    try:
                        if not filing.get("accession_number"):
                            logger.warning("Skipping filing with no accession number")
                            continue
                            
                        progress.update(
                            download_task,
                            description=f"Downloading {filing.get('form_type', 'unknown')} from {filing.get('filing_date', 'unknown date')}"
                        )
                        logger.info(f"Processing filing: {json.dumps(filing, indent=2)}")
                        
                        # Create filing directory
                        filing_dir = self.output_dir / cik_formatted / filing["accession_number"]
                        filing_dir.mkdir(parents=True, exist_ok=True)
                        logger.info(f"Created directory: {filing_dir}")
                        
                        # Download files
                        filing_files = self.download_filing(
                            cik,
                            filing["accession_number"],
                            filing_dir,
                            filing.get("files", [])
                        )
                        
                        if filing_files:
                            downloaded_files[filing["accession_number"]] = list(filing_files.values())
                            # Analyze any downloaded swap files
                            for file_path in filing_files.values():
                                if file_path.suffix.lower() in ['.csv', '.json']:
                                    logger.info(f"Found potential swap file: {file_path}. Analyzing...")
                                    self.swaps_analyzer.load_swaps_from_file(file_path, save_to_db=True)
                            logger.info(f"Successfully downloaded {len(filing_files)} files for filing {filing['accession_number']}")
                            
                            # Save filing to database
                            try:
                                logger.info("Saving filing metadata to database...")
                                self.db.cursor.execute("""
                                    INSERT INTO filings (company_cik, accession_number, form_type, filing_date, file_path)
                                    VALUES (?, ?, ?, ?, ?)
                                """, (
                                    cik,
                                    filing["accession_number"],
                                    filing.get("form_type"),
                                    filing.get("filing_date"),
                                    str(filing_dir)
                                ))
                                self.db.conn.commit()
                                logger.info("Successfully saved to database")
                            except sqlite3.IntegrityError:
                                # Filing already exists, update it
                                logger.info("Filing exists in database, updating...")
                                self.db.cursor.execute("""
                                    UPDATE filings 
                                    SET form_type = ?, filing_date = ?, file_path = ?, updated_at = datetime('now')
                                    WHERE company_cik = ? AND accession_number = ?
                                """, (
                                    filing.get("form_type"),
                                    filing.get("filing_date"),
                                    str(filing_dir),
                                    cik,
                                    filing["accession_number"]
                                ))
                                self.db.conn.commit()
                                logger.info("Successfully updated database")
                            except Exception as e:
                                logger.error(f"Failed to save filing to database: {str(e)}")
                        else:
                            logger.warning(f"No files downloaded for filing {filing['accession_number']}")
                            if filing_dir and filing_dir.exists() and not any(filing_dir.iterdir()):
                                try:
                                    filing_dir.rmdir()
                                    logger.info(f"Removed empty directory: {filing_dir}")
                                except Exception as e:
                                    logger.error(f"Error removing empty directory: {str(e)}")
                                    
                        progress.advance(download_task)
                        
                    except Exception as e:
                        logger.error(f"Error processing filing {filing.get('accession_number')}: {str(e)}")
                        continue
                        
            return downloaded_files
            
        except Exception as e:
            logger.error(f"Error downloading company filings: {str(e)}")
            return {}
