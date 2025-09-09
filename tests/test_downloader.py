"""
Tests for the downloader module.
"""
import pytest
import aiohttp
from aioresponses import aioresponses
from pathlib import Path
from rich.progress import Progress
from gamecock.downloader import SECDownloader

@pytest.fixture
def temp_dir(tmp_path):
    """Create a temporary directory for downloads."""
    return tmp_path

@pytest.fixture
def mock_aioresponse():
    """Create a mock aiohttp response."""
    with aioresponses() as m:
        yield m

# @pytest.mark.asyncio
# async def test_downloader_initialization(temp_dir):
#     """Test SECDownloader initialization."""
#     async with SECDownloader(temp_dir) as downloader:
#         assert isinstance(downloader.session, aiohttp.ClientSession)
#         assert downloader.output_dir == temp_dir
#         assert downloader.max_concurrent == 5

# @pytest.mark.asyncio
# async def test_download_file_success(temp_dir, mock_aioresponse):
#     """Test successful file download."""
#     test_url = "https://example.com/test.txt"
#     test_content = b"Test content"
#     mock_aioresponse.get(test_url, status=200, body=test_content,
#                         headers={"content-length": str(len(test_content))})
# 
#     async with SECDownloader(temp_dir) as downloader:
#         # This test needs to be updated to use download_filing
#         pass 

# @pytest.mark.asyncio
# async def test_download_file_failure(temp_dir, mock_aioresponse):
#     """Test file download failure."""
#     test_url = "https://example.com/nonexistent.txt"
#     mock_aioresponse.get(test_url, status=404)
# 
#     async with SECDownloader(temp_dir) as downloader:
#         # This test needs to be updated to use download_filing
#         pass

# @pytest.mark.asyncio
# async def test_download_multiple(temp_dir, mock_aioresponse):
#     """Test downloading multiple files."""
#     # This test needs to be rewritten for download_company_filings
#     pass
