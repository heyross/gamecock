import pytest
import json
from unittest.mock import MagicMock, patch

from gamecock.menu_system import MenuSystem


@pytest.fixture
def menu_system():
    """Create a MenuSystem instance with mock handlers."""
    db_handler = MagicMock()
    sec_handler = MagicMock()
    ollama_handler = MagicMock()
    swaps_analyzer = MagicMock()
    swaps_processor = MagicMock()
    downloader = MagicMock()
    ai_analyst = MagicMock()
    return MenuSystem(
        db_handler=db_handler,
        sec_handler=sec_handler,
        ollama_handler=ollama_handler,
        swaps_analyzer=swaps_analyzer,
        swaps_processor=swaps_processor,
        downloader=downloader,
        ai_analyst=ai_analyst
    )


def test_menu_system_initialization(menu_system):
    """Test that the MenuSystem initializes correctly."""
    assert menu_system is not None
    assert menu_system.db is not None
    assert menu_system.sec is not None
    assert menu_system.swaps_analyzer is not None

@patch('gamecock.menu_system.Prompt.ask')
def test_main_menu_navigation(mock_ask, menu_system):
    """Test main menu navigation to submenus."""
    # Mock the submenu methods to check if they are called
    menu_system.search_company_menu = MagicMock()
    menu_system.view_companies_menu = MagicMock()
    menu_system.download_filings_menu = MagicMock()
    menu_system.view_data_menu = MagicMock()
    menu_system.swaps_analysis_menu = MagicMock()
    menu_system.data_explorer_menu = MagicMock()
    menu_system._ai_analyst_menu = MagicMock()
    menu_system._reimport_data_menu = MagicMock()

    # Simulate user choosing each option and then exiting
    mock_ask.side_effect = ['1', '2', '3', '4', '5', '6', '7', '8', '0']

    # A single call to main_menu will loop until the user exits
    menu_system.main_menu()

    # Check that each submenu method was called once
    menu_system.search_company_menu.assert_called_once()
    menu_system.view_companies_menu.assert_called_once()
    menu_system.download_filings_menu.assert_called_once()
    menu_system.view_data_menu.assert_called_once()
    menu_system.swaps_analysis_menu.assert_called_once()
    menu_system.data_explorer_menu.assert_called_once()
    menu_system._ai_analyst_menu.assert_called_once()
    menu_system._reimport_data_menu.assert_called_once()


@patch('gamecock.menu_system.Prompt.ask')
@patch('gamecock.menu_system.input') # Mock input() to prevent test from hanging
def test_search_company_menu_success(mock_input, mock_ask, menu_system):
    """Test the search company menu for a successful search and save."""
    # Arrange
    company_name = 'Apple Inc.'
    mock_company_info = MagicMock()
    mock_company_info.name = 'Apple Inc.'
    mock_company_info.primary_identifiers.cik = '12345'

    menu_system.sec.get_company_info.return_value = mock_company_info
    menu_system.db.save_company.return_value = True
    menu_system._download_filings_for_company = MagicMock()

    # Simulate user input: enter company name, 'y' to save, 'n' to download
    mock_ask.side_effect = [company_name, 'y', 'n']

    # Act
    menu_system.search_company_menu()

    # Assert
    menu_system.sec.get_company_info.assert_called_once_with(company_name)
    menu_system.db.save_company.assert_called_once_with(mock_company_info)
    menu_system._download_filings_for_company.assert_not_called()


@patch('gamecock.menu_system.Prompt.ask')
@patch('gamecock.menu_system.input') # Mock input() to prevent test from hanging
def test_search_company_menu_not_found(mock_input, mock_ask, menu_system):
    """Test the search company menu when a company is not found."""
    # Arrange
    company_name = 'NonExistent Company'
    menu_system.sec.get_company_info.return_value = None

    # Simulate user input: enter company name
    mock_ask.side_effect = [company_name]

    # Act
    menu_system.search_company_menu()

    # Assert
    menu_system.sec.get_company_info.assert_called_once_with(company_name)
    menu_system.db.save_company.assert_not_called()


@patch('gamecock.menu_system.Prompt.ask')
@patch('gamecock.menu_system.input') # Mock input() to prevent test from hanging
def test_search_company_and_download(mock_input, mock_ask, menu_system):
    """Test searching for a company and then choosing to download filings."""
    # Arrange
    company_name = 'Tesla, Inc.'
    mock_company_info = MagicMock()
    mock_company_info.name = 'Tesla, Inc.'
    mock_company_info.primary_identifiers.cik = '1318605'

    menu_system.sec.get_company_info.return_value = mock_company_info
    menu_system._download_filings_for_company = MagicMock()

    # Simulate user input: company name, 'n' to save, 'y' to download
    mock_ask.side_effect = [company_name, 'n', 'y']

    # Act
    menu_system.search_company_menu()

    # Assert
    menu_system.sec.get_company_info.assert_called_once_with(company_name)
    menu_system.db.save_company.assert_not_called()
    menu_system._download_filings_for_company.assert_called_once_with(mock_company_info)


@patch('gamecock.menu_system.input')
def test_view_companies_menu_with_companies(mock_input, menu_system):
    """Test the view companies menu when there are saved companies."""
    # Arrange
    mock_company = MagicMock()
    mock_company.name = 'Test Company'
    mock_company.primary_identifiers.cik = '12345'
    mock_company.primary_identifiers.description = 'A test company.'
    mock_company.related_entities = []
    menu_system.db.get_all_companies.return_value = [mock_company]

    # Act
    menu_system.view_companies_menu()

    # Assert
    menu_system.db.get_all_companies.assert_called_once()


@patch('gamecock.menu_system.input')
def test_view_companies_menu_no_companies(mock_input, menu_system):
    """Test the view companies menu when there are no saved companies."""
    # Arrange
    menu_system.db.get_all_companies.return_value = []

    # Act
    menu_system.view_companies_menu()

    # Assert
    menu_system.db.get_all_companies.assert_called_once()


@patch('gamecock.menu_system.Prompt.ask')
@patch('gamecock.menu_system.input')
def test_download_filings_menu_no_companies(mock_input, mock_ask, menu_system):
    """Test the download filings menu when no companies are saved."""
    # Arrange
    menu_system.db.get_all_companies.return_value = []

    # Act
    menu_system.download_filings_menu()

    # Assert
    menu_system.db.get_all_companies.assert_called_once()
    mock_ask.assert_not_called()


@patch('gamecock.menu_system.Prompt.ask')
@patch('gamecock.menu_system.input')
def test_download_filings_menu_select_company(mock_input, mock_ask, menu_system):
    """Test the download filings menu with company selection."""
    # Arrange
    mock_company = MagicMock()
    menu_system.db.get_all_companies.return_value = [mock_company]
    menu_system._download_filings_for_company = MagicMock()
    mock_ask.return_value = '1'
    mock_input.return_value = 'y'

    # Act
    menu_system.download_filings_menu()

    # Assert
    menu_system.db.get_all_companies.assert_called_once()
    menu_system._download_filings_for_company.assert_called_once_with(mock_company)


@patch('gamecock.menu_system.Prompt.ask')
def test_download_filings_for_company_parent_only(mock_ask, menu_system):
    """Test downloading filings for the parent company only."""
    # Arrange
    mock_company_info = MagicMock()
    mock_company_info.primary_identifiers.cik = '12345'
    mock_company_info.name = 'TestCo'
    mock_company_info.related_entities = []  # No related entities
    mock_ask.side_effect = ['2']
    menu_system.downloader.download_company_filings.return_value = ['file1.txt']

    # Act
    menu_system._download_filings_for_company(mock_company_info)

    # Assert
    menu_system.downloader.download_company_filings.assert_called_once()


@patch('gamecock.menu_system.Prompt.ask')
def test_download_filings_for_company_with_related(mock_ask, menu_system):
    """Test downloading filings for parent and related entities."""
    # Arrange
    mock_parent_company = MagicMock()
    mock_parent_company.primary_identifiers.cik = '12345'
    mock_parent_company.name = 'ParentCo'

    mock_related_entity = MagicMock()
    mock_related_entity.cik = '67890'
    mock_related_entity.name = 'ChildCo'

    mock_parent_company.related_entities = [mock_related_entity]

    mock_ask.side_effect = ['1']  # Download all filings

    menu_system.downloader.download_company_filings.side_effect = [
        ['parent_file.txt'],  # First call returns parent files
        ['related_file.txt']  # Second call returns related files
    ]

    # Act
    menu_system._download_filings_for_company(mock_parent_company)

    # Assert
    assert menu_system.downloader.download_company_filings.call_count == 2


@patch('gamecock.menu_system.Console.print')
@patch('gamecock.menu_system.Prompt.ask')
def test_download_filings_for_company_value_error(mock_ask, mock_print, menu_system):
    """Test ValueError handling during filing download."""
    # Arrange
    mock_company_info = MagicMock()
    mock_company_info.primary_identifiers.cik = '12345'
    mock_company_info.name = 'TestCo'
    mock_company_info.related_entities = []
    mock_ask.side_effect = ['2']
    menu_system.downloader.download_company_filings.side_effect = ValueError("Test ValueError")

    # Act
    menu_system._download_filings_for_company(mock_company_info)

    # Assert
    mock_print.assert_any_call('[red]Configuration Error: Test ValueError[/red]')


@patch('gamecock.menu_system.Console.print')
@patch('gamecock.menu_system.Prompt.ask')
def test_download_filings_for_company_connection_error(mock_ask, mock_print, menu_system):
    """Test ConnectionError handling during filing download."""
    # Arrange
    mock_company_info = MagicMock()
    mock_company_info.primary_identifiers.cik = '12345'
    mock_company_info.name = 'TestCo'
    mock_company_info.related_entities = []
    mock_ask.side_effect = ['2']
    menu_system.downloader.download_company_filings.side_effect = ConnectionError("Test ConnectionError")

    # Act
    menu_system._download_filings_for_company(mock_company_info)

    # Assert
    mock_print.assert_any_call('[red]Network Error: Could not connect to SEC EDGAR. Please check your internet connection. Details: Test ConnectionError[/red]')


@patch('gamecock.menu_system.Console.print')
@patch('gamecock.menu_system.Prompt.ask')
def test_download_filings_for_company_generic_exception(mock_ask, mock_print, menu_system):
    """Test generic Exception handling during filing download."""
    # Arrange
    mock_company_info = MagicMock()
    mock_company_info.primary_identifiers.cik = '12345'
    mock_company_info.name = 'TestCo'
    mock_company_info.related_entities = []
    mock_ask.side_effect = ['2']
    menu_system.downloader.download_company_filings.side_effect = Exception("Generic Error")

    # Act
    menu_system._download_filings_for_company(mock_company_info)

    # Assert
    mock_print.assert_any_call('[red]An unexpected error occurred during download: Generic Error[/red]')


@patch('gamecock.menu_system.Prompt.ask')
@patch('pathlib.Path')
@patch('builtins.input')
def test_file_browser_navigate_parent(mock_input, mock_path, mock_ask, menu_system):
    """Test navigating to parent directory in the file browser."""
    # Arrange
    mock_start_path = MagicMock()
    mock_parent_path = MagicMock()
    mock_start_path.resolve.return_value = mock_start_path
    mock_start_path.parent = mock_parent_path
    mock_start_path.iterdir.return_value = []
    mock_parent_path.iterdir.return_value = []

    mock_ask.side_effect = ['0', 'q']  # Navigate up, then quit

    # Act
    result = menu_system._file_browser(mock_start_path)

    # Assert
    assert mock_start_path.parent is not None
    assert result is None


@patch('gamecock.menu_system.Prompt.ask')
@patch('pathlib.Path')
@patch('builtins.input')
def test_file_browser_select_file(mock_input, mock_path, mock_ask, menu_system):
    """Test selecting a file in the file browser."""
    # Arrange
    mock_file = MagicMock()
    mock_file.name = 'test_file.txt'
    mock_file.is_file.return_value = True
    mock_path.return_value.resolve.return_value.iterdir.return_value = [mock_file]
    mock_ask.return_value = '1'

    # Act
    result = menu_system._file_browser(mock_path.return_value)

    # Assert
    assert result == mock_file


@patch('gamecock.menu_system.Console.print')
@patch('gamecock.menu_system.input')
def test_reimport_data_menu_no_files(mock_input, mock_print, menu_system):
    """Test re-import menu when no files are found in the data directory."""
    # Arrange
    mock_data_dir = MagicMock()
    mock_data_dir.exists.return_value = True
    mock_data_dir.iterdir.return_value = iter([])  # Simulate an empty directory

    # Act
    menu_system._reimport_data_menu(data_dir=mock_data_dir)

    # Assert
    mock_print.assert_any_call('[yellow]No files found in the data directory to re-import.[/yellow]')
    mock_input.assert_called_once_with('\nPress Enter to continue...')


@patch('gamecock.menu_system.Console.print')
@patch('gamecock.menu_system.input')
def test_load_swaps_from_file_json_error(mock_input, mock_print, menu_system):
    """Test JSONDecodeError handling when loading swaps from a file."""
    # Arrange
    file_path = MagicMock()
    menu_system._file_browser = MagicMock(return_value=file_path)
    menu_system.swaps_analyzer.load_swaps_from_file.side_effect = json.JSONDecodeError("msg", "doc", 0)
    mock_input.return_value = ''  # To handle 'Press Enter to continue'

    # Act
    menu_system._load_swaps_from_file()

    # Assert
    mock_print.assert_any_call("[red]Error: The JSON file is malformed and could not be parsed.[/red]")
    mock_input.assert_called_once()


@patch('gamecock.menu_system.Prompt.ask')
@patch('builtins.input')
def test_reimport_data_menu_user_declines(mock_input, mock_ask, menu_system):
    """Test re-import menu when the user declines."""
    # Arrange
    mock_data_dir = MagicMock()
    mock_data_dir.exists.return_value = True
    mock_data_dir.iterdir.return_value = [MagicMock()]  # Simulate a non-empty directory
    mock_ask.return_value = 'n'
    menu_system.swaps_processor.process_directory = MagicMock()

    # Act
    menu_system._reimport_data_menu(data_dir=mock_data_dir)

    # Assert
    menu_system.swaps_processor.process_directory.assert_not_called()


@patch('gamecock.menu_system.Prompt.ask')
@patch('builtins.input')
@patch('gamecock.menu_system.Console.print')
def test_file_browser_invalid_selection(mock_print, mock_input, mock_ask, menu_system):
    """Test invalid selection in the file browser."""
    # Arrange
    mock_path = MagicMock()
    mock_path.resolve.return_value = mock_path
    mock_path.iterdir.return_value = []
    mock_ask.side_effect = ['invalid', 'q']  # Invalid input, then quit

    # Act
    menu_system._file_browser(mock_path)

    # Assert
    mock_print.assert_any_call("[red]Invalid input: invalid[/red]")


@patch('gamecock.menu_system.Prompt.ask')
def test_search_company_menu_empty_input(mock_ask, menu_system):
    """Test the search company menu with empty input."""
    # Arrange
    mock_ask.return_value = ''
    menu_system.sec.get_company_info = MagicMock()

    # Act
    menu_system.search_company_menu()

    # Assert
    menu_system.sec.get_company_info.assert_not_called()
