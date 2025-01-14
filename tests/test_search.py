"""
Tests for the search module.
"""
import pytest
import re
from pathlib import Path
from gamecock.search import SECSearcher, SearchResult

@pytest.fixture
def temp_dir(tmp_path):
    """Create a temporary directory with test files."""
    # Create test files
    file1 = tmp_path / "test1.txt"
    file1.write_text("""This is a test file
with multiple lines
containing test data
and some more lines
for testing purposes""")

    file2 = tmp_path / "test2.txt"
    file2.write_text("""Another test file
with different content
but also for testing""")

    return tmp_path

def test_search_result_creation():
    """Test creating a SearchResult instance."""
    result = SearchResult(
        file_path=Path("test.txt"),
        line_number=1,
        content="test line",
        context="test context",
        form_type="10-K"
    )
    assert result.file_path == Path("test.txt")
    assert result.line_number == 1
    assert result.content == "test line"
    assert result.context == "test context"
    assert result.form_type == "10-K"

def test_search_file_string_pattern(temp_dir):
    """Test searching a file with string pattern."""
    searcher = SECSearcher(temp_dir)
    results = searcher.search_file(temp_dir / "test1.txt", "test")
    
    # Count actual occurrences of "test" in the content
    test_count = sum(1 for r in results if "test" in r.content.lower())
    assert test_count == 3  # Should find "test" three times in actual content

def test_search_file_regex_pattern(temp_dir):
    """Test searching a file with regex pattern."""
    searcher = SECSearcher(temp_dir)
    pattern = re.compile(r"test\w*", re.IGNORECASE)
    results = searcher.search_file(temp_dir / "test1.txt", pattern)
    
    assert len(results) > 0
    assert all(pattern.search(r.content) for r in results)

def test_search_all(temp_dir):
    """Test searching all files."""
    searcher = SECSearcher(temp_dir)
    results = list(searcher.search_all("test"))
    
    # Count actual occurrences of "test" in both files
    test_count = sum(1 for r in results if "test" in r.content.lower())
    assert test_count > 0
    assert all(isinstance(r, SearchResult) for r in results)
    
    # Should find results in both test files
    found_files = {r.file_path.name for r in results}
    assert "test1.txt" in found_files
    assert "test2.txt" in found_files

def test_search_with_context(temp_dir):
    """Test searching with context lines."""
    searcher = SECSearcher(temp_dir)
    results = searcher.search_file(temp_dir / "test1.txt", "test", context_lines=1)
    
    for result in results:
        # Context should include the matching line plus one line before and after
        assert len(result.context.splitlines()) <= 3

def test_search_nonexistent_file(temp_dir):
    """Test searching a nonexistent file."""
    searcher = SECSearcher(temp_dir)
    results = searcher.search_file(temp_dir / "nonexistent.txt", "test")
    assert len(results) == 0

def test_search_with_file_pattern(temp_dir):
    """Test searching with specific file pattern."""
    # Create a non-matching file
    (temp_dir / "test.dat").write_text("test content")
    
    searcher = SECSearcher(temp_dir)
    results = list(searcher.search_all("test", file_pattern="*.txt"))
    
    # Should only find results in .txt files
    found_files = {r.file_path.name for r in results}
    assert all(f.endswith(".txt") for f in found_files)
    assert not any(f.endswith(".dat") for f in found_files)
