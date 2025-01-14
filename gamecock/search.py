"""
Search functionality for SEC filings.
"""
import re
from pathlib import Path
from typing import List, Dict, Generator, Union
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from loguru import logger

@dataclass
class SearchResult:
    """Represents a search result from SEC filings."""
    file_path: Path
    line_number: int
    content: str
    context: str
    form_type: str

class SECSearcher:
    """Search engine for SEC filings."""
    
    def __init__(self, base_dir: Path, max_workers: int = None):
        self.base_dir = Path(base_dir)
        self.max_workers = max_workers
        
    def search_file(self, file_path: Path, 
                   pattern: Union[str, re.Pattern],
                   context_lines: int = 2) -> List[SearchResult]:
        """Search a single file for pattern matches."""
        results = []
        try:
            content = file_path.read_text(encoding='utf-8', errors='ignore')
            lines = content.splitlines()
            
            for i, line in enumerate(lines):
                if isinstance(pattern, str):
                    if pattern.lower() in line.lower():
                        # Get context lines
                        start = max(0, i - context_lines)
                        end = min(len(lines), i + context_lines + 1)
                        context = '\n'.join(lines[start:end])
                        
                        # Determine form type from path
                        form_type = self._extract_form_type(file_path)
                        
                        results.append(SearchResult(
                            file_path=file_path,
                            line_number=i + 1,
                            content=line,
                            context=context,
                            form_type=form_type
                        ))
                else:
                    match = pattern.search(line)
                    if match:
                        # Get context lines
                        start = max(0, i - context_lines)
                        end = min(len(lines), i + context_lines + 1)
                        context = '\n'.join(lines[start:end])
                        
                        # Determine form type from path
                        form_type = self._extract_form_type(file_path)
                        
                        results.append(SearchResult(
                            file_path=file_path,
                            line_number=i + 1,
                            content=line,
                            context=context,
                            form_type=form_type
                        ))
                    
        except Exception as e:
            logger.error(f"Error searching {file_path}: {str(e)}")
            
        return results

    def _search_file_wrapper(self, args):
        """Wrapper function for multiprocessing."""
        file_path, pattern = args
        return self.search_file(file_path, pattern)
        
    def search_all(self, pattern: str, 
                   file_pattern: str = "*.txt") -> Generator[SearchResult, None, None]:
        """Search all matching files for pattern."""
        files = list(self.base_dir.rglob(file_pattern))
        
        # Compile regex if pattern looks like regex
        if any(c in pattern for c in '.^$*+?{}[]\\|()'): 
            try:
                pattern = re.compile(pattern, re.IGNORECASE)
            except re.error:
                pattern = pattern.lower()
        else:
            pattern = pattern.lower()
            
        search_args = [(f, pattern) for f in files]
        
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            for result_list in executor.map(self._search_file_wrapper, search_args):
                yield from result_list
                
    def _extract_form_type(self, file_path: Path) -> str:
        """Extract SEC form type from file path."""
        # Implement form type extraction logic
        return "Unknown"
