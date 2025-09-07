from gamecock.downloader import SECDownloader
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
from pathlib import Path

# Load environment variables from .env file
load_dotenv()

# Set output directory
output_dir = Path(__file__).parent / 'downloads'
output_dir.mkdir(parents=True, exist_ok=True)

# Create downloader instance
downloader = SECDownloader(output_dir=output_dir)

# Set date range (last 30 days)
end_date = datetime.now()
start_date = end_date - timedelta(days=30)

# Download filings for GameStop
cik = '1326380'
filing_types = ['10-K', '10-Q', '8-K', '4']  # Common filing types

print(f"Downloading filings for GameStop (CIK: {cik})")
print(f"Date range: {start_date.date()} to {end_date.date()}")
print(f"Filing types: {filing_types}")

# Download the filings
files = downloader.download_company_filings(
    cik=cik,
    start_date=start_date,
    end_date=end_date,
    filing_types=filing_types
)

print("\nDownloaded files:")
for accession, file_list in files.items():
    print(f"\nAccession number: {accession}")
    for file in file_list:
        print(f"  - {file}")
