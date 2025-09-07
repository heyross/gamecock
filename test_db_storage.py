"""Test the downloader's database storage functionality."""
from gamecock.downloader import SECDownloader
from pathlib import Path
import logging
from loguru import logger
import sys
import sqlite3

# Configure logging
logger.remove()  # Remove default handler
logger.add(sys.stderr, level="DEBUG")  # Add handler with DEBUG level

DB_PATH = Path(__file__).parent / 'data' / 'sec_data.db'

def verify_database_entry(cik, accession):
    """Verify that filing data was stored correctly in the database."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        # Check filings table
        cursor.execute("""
            SELECT * FROM filings 
            WHERE company_cik = ? AND accession_number = ?
        """, (cik, accession))
        filing = cursor.fetchone()
        
        if filing:
            logger.success(f"Found filing in database: {filing}")
        else:
            logger.error(f"Filing not found in database for CIK {cik} and accession {accession}")
        
        # Check filing_files table
        cursor.execute("""
            SELECT * FROM filing_files 
            WHERE filing_id = (
                SELECT id FROM filings 
                WHERE company_cik = ? AND accession_number = ?
            )
        """, (cik, accession))
        files = cursor.fetchall()
        
        if files:
            logger.success(f"Found {len(files)} files in database")
            for file in files:
                logger.info(f"- {file}")
        else:
            logger.error("No files found in database for this filing")
        
        return filing is not None and len(files) > 0
    finally:
        cursor.close()
        conn.close()

def test_database_storage():
    """Test downloading a filing and verify database storage."""
    # Create downloader instance
    output_dir = Path("test_downloads_db")
    downloader = SECDownloader(output_dir, db_path=DB_PATH)
    
    # GameStop filing details
    cik = "1326380"
    accession = "0001326380-23-000034"
    
    # Try to download the filing
    logger.info(f"\nTesting download and database storage for GameStop filing {accession}")
    files = downloader.download_filing(cik, accession)
    
    if files:
        logger.success(f"Successfully downloaded {len(files)} files")
        
        # Verify database storage
        if verify_database_entry(cik, accession):
            logger.success("Database storage verification successful")
        else:
            logger.error("Database storage verification failed")
    else:
        logger.error("Failed to download files")

if __name__ == "__main__":
    test_database_storage()
