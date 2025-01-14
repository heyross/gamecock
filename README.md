# Gamecock - SEC Filing Analysis Tool

A powerful tool for downloading, searching, and analyzing SEC filings. This tool allows you to scrape and analyze various types of SEC archives, making SEC filing analysis accessible to everyone.

## Features

- **Comprehensive SEC Form Support**: Downloads and processes multiple SEC form types:
  - Annual Reports (10-K, 10-K/A)
  - Quarterly Reports (10-Q, 10-Q/A)
  - Current Reports (8-K, 8-K/A)
  - Proxy Statements (DEF 14A)
  - Registration Statements (S-1, F-1)
  - Investment Company Forms (N-1A, N-CSR)
  - Insider Trading Forms (3, 4, 5)
  - And many more...

- **Advanced Search Capabilities**:
  - Full-text search across all downloaded filings
  - Regular expression support
  - Context-aware results with surrounding text
  - File type filtering

- **Efficient Downloads**:
  - Asynchronous downloading for better performance
  - Automatic retry on failure
  - Progress tracking
  - Concurrent downloads

- **Modern CLI Interface**:
  - Rich text formatting
  - Progress bars
  - Interactive menus
  - Detailed error messages

## System Requirements

- Python 3.10 or higher
- Windows 10/11 (recommended)
- Minimum 8GB RAM (16GB+ recommended for large datasets)
- Storage space depends on the number of filings downloaded

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/gamecock.git
cd gamecock
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Command Line Interface

The tool provides several commands through its CLI:

1. **Basic Usage**:
```bash
python -m gamecock.cli
```

2. **Search Command**:
```bash
python -m gamecock.cli search "search term" --dir /path/to/files
```

3. **Download Command**:
```bash
python -m gamecock.cli download --output /download/path --concurrent 5
```

### Interactive Menu Options

When running the tool interactively, you'll see these main options:

1. **Learn about SEC Forms pt. 6**
   - Comprehensive information about common SEC forms
   - Descriptions and use cases
   - Links to Investopedia references

2. **Learn about SEC Forms pt. 9**
   - Additional SEC form information
   - Advanced filing types
   - Regulatory requirements

3. **Learn about Market Instruments pt. 420**
   - Market instrument definitions
   - Trading concepts
   - Financial instrument analysis

4. **Quit**
   - Exit the program

### Archive Types Available for Download

- Form D Archives
- NCEN Archives
- NPORT Archives
- 13F Archives
- NMFP Archives
- Exchange Archives
- Insider Trading Archives
- Credit Archives
- Equity Archives
- EDGAR Archives

## Advanced Features

### Search Options

- **Pattern Matching**:
  ```bash
  python -m gamecock.cli search "pattern" --dir /path
  ```

- **File Type Filtering**:
  ```bash
  python -m gamecock.cli search "pattern" --dir /path --pattern "*.txt"
  ```

### Download Options

- **Concurrent Downloads**:
  ```bash
  python -m gamecock.cli download --concurrent 10
  ```

- **Custom Output Directory**:
  ```bash
  python -m gamecock.cli download --output /custom/path
  ```

## Development

### Running Tests

```bash
pytest
```

### Code Coverage

```bash
pytest --cov=gamecock --cov-report=term-missing
```

## Troubleshooting

### Common Issues

1. **Download Failures**
   - Check your internet connection
   - Verify SEC EDGAR system status
   - Try reducing concurrent downloads

2. **Search Issues**
   - Ensure files are properly downloaded
   - Check file permissions
   - Verify search pattern syntax

3. **Memory Issues**
   - Reduce concurrent operations
   - Process smaller batches
   - Free up system memory

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- SEC EDGAR system for providing access to filings
- Python community for excellent libraries
- Contributors and users for feedback and improvements

## Version History

- 2.0.0 - Major refactor with improved architecture
- 1.0.0 - Initial release

## Contact

For issues and feature requests, please use the GitHub issue tracker.
