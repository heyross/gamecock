"""Test downloading a real filing using the main downloader."""
from gamecock.downloader import SECDownloader
from pathlib import Path
import logging
from loguru import logger
import sys

# Configure logging
logger.remove()  # Remove default handler
logger.add(sys.stderr, level="DEBUG")  # Add handler with DEBUG level

def test_real_filing():
    """Test downloading a known working filing."""
    # Create downloader instance
    output_dir = Path("test_downloads_real")
    downloader = SECDownloader(output_dir)
    
    # GameStop filing details
    cik = "1326380"
    accession = "0001326380-23-000034"
    
    # Try to download the filing
    logger.info(f"\nTesting download of GameStop filing {accession}")
    files = downloader.download_filing(cik, accession)
    
    if files:
        logger.success(f"Successfully downloaded {len(files)} files:")
        for f in files:
            logger.info(f"- {f.name}")
    else:
        logger.error("Failed to download any files")

if __name__ == "__main__":
    test_real_filing()
