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

@pytest.mark.asyncio
async def test_downloader_initialization(temp_dir):
    """Test SECDownloader initialization."""
    async with SECDownloader(temp_dir) as downloader:
        assert isinstance(downloader.session, aiohttp.ClientSession)
        assert downloader.base_dir == temp_dir
        assert downloader.max_concurrent == 5

@pytest.mark.asyncio
async def test_download_file_success(temp_dir, mock_aioresponse):
    """Test successful file download."""
    test_url = "https://example.com/test.txt"
    test_content = b"Test content"
    mock_aioresponse.get(test_url, status=200, body=test_content,
                        headers={"content-length": str(len(test_content))})

    async with SECDownloader(temp_dir) as downloader:
        with Progress() as progress:
            task_id = progress.add_task("Testing", total=100)
            success = await downloader.download_file(
                test_url,
                temp_dir / "test.txt",
                progress,
                task_id
            )
        
        assert success
        assert (temp_dir / "test.txt").exists()
        assert (temp_dir / "test.txt").read_bytes() == test_content

@pytest.mark.asyncio
async def test_download_file_failure(temp_dir, mock_aioresponse):
    """Test file download failure."""
    test_url = "https://example.com/nonexistent.txt"
    mock_aioresponse.get(test_url, status=404)

    async with SECDownloader(temp_dir) as downloader:
        with Progress() as progress:
            task_id = progress.add_task("Testing", total=100)
            success = await downloader.download_file(
                test_url,
                temp_dir / "nonexistent.txt",
                progress,
                task_id
            )
        
        assert not success
        assert not (temp_dir / "nonexistent.txt").exists()

@pytest.mark.asyncio
async def test_download_multiple(temp_dir, mock_aioresponse):
    """Test downloading multiple files."""
    urls = [
        "https://example.com/file1.txt",
        "https://example.com/file2.txt"
    ]
    subdirs = ["dir1", "dir2"]
    
    for url in urls:
        mock_aioresponse.get(url, status=200, body=b"Test content",
                            headers={"content-length": "12"})

    async with SECDownloader(temp_dir) as downloader:
        await downloader.download_multiple(urls, subdirs)
        
        assert (temp_dir / "dir1" / "file1.txt").exists()
        assert (temp_dir / "dir2" / "file2.txt").exists()
