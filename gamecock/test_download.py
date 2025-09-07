"""Test module to try different approaches for downloading SEC filings."""
import os
from pathlib import Path
import requests
import json
from loguru import logger
import sys
import time
from dotenv import load_dotenv

# Configure logging
logger.remove()  # Remove default handler
logger.add(sys.stderr, level="DEBUG")  # Add handler with DEBUG level

class SECDownloadTester:
    def __init__(self):
        # Load environment variables
        load_dotenv()
        
        # Set headers with SEC_USER_AGENT from .env
        self.headers = {
            'User-Agent': os.getenv('SEC_USER_AGENT'),
            'Accept-Encoding': 'gzip, deflate'
        }
        
        if not self.headers['User-Agent']:
            raise ValueError("SEC_USER_AGENT must be set in .env file")
            
        # Test filing details - using GameStop's real CIK and a recent filing
        self.test_cik = "1326380"  # GameStop Corp
        self.test_accession = "0001326380-23-000034"  # A real filing from GameStop
        self.output_dir = Path("test_downloads")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Rate limiting
        self.min_request_interval = 0.1
        self.last_request_time = 0
        
    def _wait_for_rate_limit(self):
        """Ensure we don't exceed SEC's rate limit."""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.min_request_interval:
            time.sleep(self.min_request_interval - time_since_last)
        self.last_request_time = time.time()
        
    def _make_request(self, url: str, host_header: str = None) -> requests.Response:
        """Make a request with detailed logging."""
        self._wait_for_rate_limit()
        
        headers = self.headers.copy()
        if host_header:
            headers['Host'] = host_header
            
        logger.debug(f"Making request to {url}")
        logger.debug(f"Using headers: {headers}")
        
        try:
            response = requests.get(url, headers=headers, timeout=10)
            logger.debug(f"Response status: {response.status_code}")
            logger.debug(f"Response headers: {dict(response.headers)}")
            if response.status_code != 200:
                logger.debug(f"Response content: {response.text[:500]}")
                
            # Handle redirects
            if response.status_code == 301 or response.status_code == 302:
                redirect_url = response.headers.get('Location')
                if redirect_url:
                    logger.info(f"Following redirect to {redirect_url}")
                    return self._make_request(redirect_url, host_header)
                    
            return response
        except Exception as e:
            logger.error(f"Request failed: {str(e)}")
            return None
            
    def test_index_url_patterns(self):
        """Test different patterns for accessing the index.json file."""
        patterns = [
            # Pattern 1: Standard format
            f"https://www.sec.gov/Archives/edgar/data/{self.test_cik}/{self.test_accession.replace('-', '')}/index.json",
            
            # Pattern 2: With leading zeros in CIK
            f"https://www.sec.gov/Archives/edgar/data/{self.test_cik.zfill(10)}/{self.test_accession.replace('-', '')}/index.json",
            
            # Pattern 3: Using data.sec.gov
            f"https://data.sec.gov/Archives/edgar/data/{self.test_cik}/{self.test_accession.replace('-', '')}/index.json",
            
            # Pattern 4: Without /Archives prefix
            f"https://www.sec.gov/edgar/data/{self.test_cik}/{self.test_accession.replace('-', '')}/index.json",
            
            # Pattern 5: All lowercase
            f"https://www.sec.gov/archives/edgar/data/{self.test_cik}/{self.test_accession.replace('-', '')}/index.json",
            
            # Pattern 6: Try without dashes in accession
            f"https://www.sec.gov/Archives/edgar/data/{self.test_cik}/{self.test_accession.replace('-', '')}/index.json",
            
            # Pattern 7: Try with dashes in accession
            f"https://www.sec.gov/Archives/edgar/data/{self.test_cik}/{self.test_accession}/index.json",
        ]
        
        logger.info("Testing index.json URL patterns...")
        for pattern in patterns:
            logger.info(f"\nTrying pattern: {pattern}")
            
            # Try with www.sec.gov host header
            response = self._make_request(pattern, "www.sec.gov")
            if response and response.status_code == 200:
                logger.success(f"Success with www.sec.gov host header: {pattern}")
                try:
                    data = response.json()
                    logger.info(f"Found {len(data.get('directory', {}).get('item', []))} files")
                    return pattern, data
                except Exception as e:
                    logger.error(f"Failed to parse JSON: {str(e)}")
                    
            # Try with data.sec.gov host header
            response = self._make_request(pattern, "data.sec.gov")
            if response and response.status_code == 200:
                logger.success(f"Success with data.sec.gov host header: {pattern}")
                try:
                    data = response.json()
                    logger.info(f"Found {len(data.get('directory', {}).get('item', []))} files")
                    return pattern, data
                except Exception as e:
                    logger.error(f"Failed to parse JSON: {str(e)}")
                    
            # Try without host header
            response = self._make_request(pattern)
            if response and response.status_code == 200:
                logger.success(f"Success without host header: {pattern}")
                try:
                    data = response.json()
                    logger.info(f"Found {len(data.get('directory', {}).get('item', []))} files")
                    return pattern, data
                except Exception as e:
                    logger.error(f"Failed to parse JSON: {str(e)}")
                    
        logger.error("All index.json patterns failed")
        return None, None
        
    def test_file_url_patterns(self, file_name: str):
        """Test different patterns for downloading a specific file."""
        patterns = [
            # Pattern 1: Standard format
            f"https://www.sec.gov/Archives/edgar/data/{self.test_cik}/{self.test_accession.replace('-', '')}/{file_name}",
            
            # Pattern 2: With leading zeros in CIK
            f"https://www.sec.gov/Archives/edgar/data/{self.test_cik.zfill(10)}/{self.test_accession.replace('-', '')}/{file_name}",
            
            # Pattern 3: Using data.sec.gov
            f"https://data.sec.gov/Archives/edgar/data/{self.test_cik}/{self.test_accession.replace('-', '')}/{file_name}",
            
            # Pattern 4: Without /Archives prefix
            f"https://www.sec.gov/edgar/data/{self.test_cik}/{self.test_accession.replace('-', '')}/{file_name}",
            
            # Pattern 5: All lowercase
            f"https://www.sec.gov/archives/edgar/data/{self.test_cik}/{self.test_accession.replace('-', '')}/{file_name}",
            
            # Pattern 6: Full path from root
            f"https://www.sec.gov/Archives/edgar/data/{self.test_cik}/{self.test_accession.replace('-', '')}/{file_name}",
        ]
        
        logger.info(f"\nTesting download patterns for file: {file_name}")
        for pattern in patterns:
            logger.info(f"\nTrying pattern: {pattern}")
            
            # Try with www.sec.gov host header
            response = self._make_request(pattern, "www.sec.gov")
            if response and response.status_code == 200:
                logger.success(f"Success with www.sec.gov host header: {pattern}")
                return pattern, response
                
            # Try with data.sec.gov host header
            response = self._make_request(pattern, "data.sec.gov")
            if response and response.status_code == 200:
                logger.success(f"Success with data.sec.gov host header: {pattern}")
                return pattern, response
                
            # Try without host header
            response = self._make_request(pattern)
            if response and response.status_code == 200:
                logger.success(f"Success without host header: {pattern}")
                return pattern, response
                
        logger.error(f"All download patterns failed for {file_name}")
        return None, None
        
    def run_tests(self):
        """Run all tests to find working patterns."""
        logger.info("Starting SEC download pattern tests...")
        
        # Step 1: Test index.json access
        logger.info("\nStep 1: Testing index.json access")
        index_pattern, index_data = self.test_index_url_patterns()
        
        if not index_pattern or not index_data:
            logger.error("Failed to find working pattern for index.json")
            return
            
        logger.success(f"Found working index.json pattern: {index_pattern}")
        
        # Step 2: Get list of files
        logger.info("\nStep 2: Getting file list")
        try:
            files = index_data.get('directory', {}).get('item', [])
            logger.info(f"Found {len(files)} files in index")
            
            if not files:
                logger.error("No files found in index")
                return
                
            # Try to download each file
            for file_info in files:
                if not isinstance(file_info, dict):
                    continue
                    
                file_name = file_info.get('name')
                if not file_name:
                    continue
                    
                logger.info(f"\nTesting download patterns for: {file_name}")
                file_pattern, response = self.test_file_url_patterns(file_name)
                
                if file_pattern and response:
                    logger.success(f"Found working pattern for {file_name}: {file_pattern}")
                    
                    # Try to save the file
                    try:
                        output_path = self.output_dir / file_name
                        output_path.parent.mkdir(parents=True, exist_ok=True)
                        
                        with open(output_path, 'wb') as f:
                            for chunk in response.iter_content(chunk_size=8192):
                                if chunk:
                                    f.write(chunk)
                                    
                        if output_path.exists() and output_path.stat().st_size > 0:
                            logger.success(f"Successfully saved file to {output_path}")
                        else:
                            logger.error(f"Failed to save file {file_name}")
                            
                    except Exception as e:
                        logger.error(f"Error saving file {file_name}: {str(e)}")
                        
        except Exception as e:
            logger.error(f"Error processing index data: {str(e)}")
            
def main():
    """Main entry point."""
    try:
        tester = SECDownloadTester()
        tester.run_tests()
    except Exception as e:
        logger.error(f"Test failed: {str(e)}")
        
if __name__ == "__main__":
    main()
