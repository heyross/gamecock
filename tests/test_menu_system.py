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


@patch('gamecock.menu_system.Prompt.ask')
@patch('gamecock.menu_system.input')
@patch('gamecock.menu_system.Console.print')
def test_search_company_menu_save_fails(mock_print, mock_input, mock_ask, menu_system):
    """Test the search company menu when saving the company fails."""
    # Arrange
    company_name = 'Apple Inc.'
    mock_company_info = MagicMock()
    menu_system.sec.get_company_info.return_value = mock_company_info
    menu_system.db.save_company.return_value = False  # Simulate save failure
    mock_ask.side_effect = [company_name, 'y', 'n']

    # Act
    menu_system.search_company_menu()

    # Assert
    menu_system.db.save_company.assert_called_once_with(mock_company_info)
    mock_print.assert_any_call('[red]Failed to save company.[/red]')


@patch('gamecock.menu_system.Console.print')
def test_display_company_info_with_dict_ticker(mock_print, menu_system):
    """Test displaying company info with a ticker stored as a dictionary."""
    # Arrange
    mock_company_info = MagicMock()
    mock_company_info.name = 'Dict Ticker Co'
    mock_company_info.primary_identifiers.cik = '54321'
    mock_company_info.primary_identifiers.tickers = [{'symbol': 'DTC', 'exchange': 'NYSE'}]
    mock_company_info.related_entities = []

    # Act
    menu_system.display_company_info(mock_company_info)

    # Assert
    mock_print.assert_any_call('Ticker: DTC (NYSE)')


@patch('gamecock.menu_system.Console.print')
def test_display_company_info_with_string_ticker(mock_print, menu_system):
    """Test displaying company info with a ticker stored as a string."""
    # Arrange
    mock_company_info = MagicMock()
    mock_company_info.name = 'String Ticker Co'
    mock_company_info.primary_identifiers.cik = '98765'
    mock_company_info.primary_identifiers.tickers = ['STC']
    mock_company_info.related_entities = []

    # Act
    menu_system.display_company_info(mock_company_info)

    # Assert
    mock_print.assert_any_call('Ticker: STC')


@patch('gamecock.menu_system.Console.print')
def test_display_company_info_with_related_entities(mock_print, menu_system):
    """Test displaying company info with related entities."""
    # Arrange
    mock_related_entity = MagicMock()
    mock_related_entity.name = 'Related Inc.'
    mock_related_entity.cik = '11223'

    mock_company_info = MagicMock()
    mock_company_info.name = 'Parent Co'
    mock_company_info.primary_identifiers.cik = '44556'
    mock_company_info.primary_identifiers.tickers = []
    mock_company_info.related_entities = [mock_related_entity]

    # Act
    menu_system.display_company_info(mock_company_info)

    # Assert
    mock_print.assert_any_call('- Related Inc. (CIK: 11223)')


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


@patch('gamecock.menu_system.input')
def test_view_data_menu_success(mock_input, menu_system):
    """Test the view data menu successfully displays statistics."""
    # Arrange
    mock_cursor = MagicMock()
    mock_cursor.fetchone.side_effect = [(10,), (5,), ('2023-01-01',)]
    mock_cursor.fetchall.return_value = [('10-K', 8), ('10-Q', 2)]
    menu_system.db.cursor = mock_cursor

    # Act
    menu_system.view_data_menu()

    # Assert
    assert mock_cursor.execute.call_count == 4
    mock_input.assert_called_once()


@patch('gamecock.menu_system.input')
@patch('gamecock.menu_system.Console.print')
def test_view_data_menu_exception(mock_print, mock_input, menu_system):
    """Test the view data menu when a database exception occurs."""
    # Arrange
    menu_system.db.cursor.execute.side_effect = Exception("DB Error")

    # Act
    menu_system.view_data_menu()

    # Assert
    mock_print.assert_any_call('[red]Error getting statistics: DB Error[/red]')
    mock_input.assert_called_once()


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


@patch('gamecock.menu_system.input')
@patch('gamecock.menu_system.Prompt.ask')
def test_generate_risk_report_empty_input(mock_ask, mock_input, menu_system):
    """Test generating a risk report with empty user input."""
    # Arrange
    mock_ask.return_value = ''
    menu_system.swaps_analyzer.generate_risk_report = MagicMock()
    mock_input.return_value = ''  # prevent stdin read at end

    # Act
    menu_system._generate_risk_report()

    # Assert
    menu_system.swaps_analyzer.generate_risk_report.assert_not_called()


@patch('gamecock.menu_system.Prompt.ask')
@patch('gamecock.menu_system.input')
@patch('gamecock.menu_system.Console.print')
def test_generate_risk_report_exception(mock_print, mock_input, mock_ask, menu_system):
    """Test exception handling during risk report generation."""
    # Arrange
    mock_ask.return_value = 'Test Entity'
    menu_system.swaps_analyzer.generate_risk_report.side_effect = Exception('Report Error')

    # Act
    menu_system._generate_risk_report()

    # Assert
    mock_print.assert_any_call('[red]Error generating risk report: Report Error[/red]')


@patch('gamecock.menu_system.Prompt.ask')
@patch('gamecock.menu_system.input')
def test_export_swaps_data_success(mock_input, mock_ask, menu_system):
    """Test exporting swaps data successfully."""
    # Arrange
    menu_system.swaps_analyzer.swaps = [MagicMock()]
    mock_ask.return_value = 'export.csv'
    menu_system.swaps_analyzer.export_to_csv.return_value = True

    # Act
    menu_system._export_swaps_data()

    # Assert
    menu_system.swaps_analyzer.export_to_csv.assert_called_once_with('export.csv')


@patch('gamecock.menu_system.Prompt.ask')
def test_export_swaps_data_no_path(mock_ask, menu_system):
    """Test exporting swaps data when the user provides no path."""
    # Arrange
    menu_system.swaps_analyzer.swaps = [MagicMock()]
    mock_ask.return_value = ''
    menu_system.swaps_analyzer.export_to_csv = MagicMock()

    # Act
    menu_system._export_swaps_data()

    # Assert
    menu_system.swaps_analyzer.export_to_csv.assert_not_called()


@patch('gamecock.menu_system.input')
@patch('gamecock.menu_system.Console.print')
def test_export_swaps_data_no_swaps(mock_print, mock_input, menu_system):
    """Test exporting swaps data when no swaps are loaded."""
    # Arrange
    menu_system.swaps_analyzer.swaps = []

    # Act
    menu_system._export_swaps_data()

    # Assert
    mock_print.assert_any_call('[yellow]No swaps loaded to export.[/yellow]')


@patch('gamecock.menu_system.Prompt.ask')
@patch('gamecock.menu_system.input')
@patch('gamecock.menu_system.Console.print')
def test_export_swaps_data_failure(mock_print, mock_input, mock_ask, menu_system):
    """Test exporting swaps data when the export fails."""
    # Arrange
    menu_system.swaps_analyzer.swaps = [MagicMock()]
    mock_ask.return_value = 'export.csv'
    menu_system.swaps_analyzer.export_to_csv.return_value = False

    # Act
    menu_system._export_swaps_data()

    # Assert
    mock_print.assert_any_call('[red]Failed to export swaps data.[/red]')


@patch('gamecock.menu_system.Prompt.ask')
@patch('gamecock.menu_system.input')
@patch('gamecock.menu_system.Console.print')
def test_export_swaps_data_exception(mock_print, mock_input, mock_ask, menu_system):
    """Test exception handling during swaps export."""
    # Arrange
    menu_system.swaps_analyzer.swaps = [MagicMock()]
    mock_ask.return_value = 'export.csv'
    menu_system.swaps_analyzer.export_to_csv.side_effect = Exception('Export Error')

    # Act
    menu_system._export_swaps_data()

    # Assert
    mock_print.assert_any_call('[red]Error exporting swaps: Export Error[/red]')


@patch('gamecock.menu_system.Prompt.ask')
def test_data_explorer_menu_navigation(mock_ask, menu_system):
    """Test navigation in the data explorer menu."""
    # Arrange
    menu_system._list_all_counterparties = MagicMock()
    menu_system._list_all_reference_securities = MagicMock()
    mock_ask.side_effect = ['1', '2', '0']

    # Act
    menu_system.data_explorer_menu()

    # Assert
    menu_system._list_all_counterparties.assert_called_once()
    menu_system._list_all_reference_securities.assert_called_once()


@patch('gamecock.menu_system.input')
@patch('gamecock.menu_system.Console.print')
def test_list_all_counterparties_no_data(mock_print, mock_input, menu_system):
    """Test listing counterparties when none are in the database."""
    # Arrange
    menu_system.db.get_all_counterparties.return_value = []

    # Act
    menu_system._list_all_counterparties()

    # Assert
    mock_print.assert_any_call('[yellow]No counterparties found in the database.[/yellow]')


@patch('gamecock.menu_system.Prompt.ask')
def test_list_all_counterparties_with_data(mock_ask, menu_system):
    """Test listing counterparties and selecting one to view swaps."""
    # Arrange
    counterparties = [{'id': 1, 'name': 'CP1', 'lei': 'LEI1'}]
    menu_system.db.get_all_counterparties.return_value = counterparties
    mock_ask.return_value = '1'
    menu_system._view_swaps_for_counterparty = MagicMock()

    # Act
    menu_system._list_all_counterparties()

    # Assert
    menu_system._view_swaps_for_counterparty.assert_called_once_with(1)


@patch('gamecock.menu_system.input')
@patch('gamecock.menu_system.Console.print')
def test_view_swaps_for_counterparty_no_swaps(mock_print, mock_input, menu_system):
    """Test viewing swaps for a counterparty that has no swaps."""
    # Arrange
    menu_system.db.get_swaps_by_counterparty_id.return_value = []

    # Act
    menu_system._view_swaps_for_counterparty(1)

    # Assert
    mock_print.assert_any_call('[yellow]No swaps found for counterparty ID 1.[/yellow]')


@patch('gamecock.menu_system.Prompt.ask')
@patch('gamecock.menu_system.input')
def test_view_swaps_for_counterparty_with_swaps(mock_input, mock_ask, menu_system):
    """Test viewing swaps for a counterparty and explaining one."""
    # Arrange
    swaps = [{'contract_id': 'c1', 'reference_entity': 'RE1', 'currency': 'USD', 'notional_amount': 100, 'maturity_date': '2023-01-01'}]
    menu_system.db.get_swaps_by_counterparty_id.return_value = swaps
    mock_ask.return_value = 'c1'
    menu_system._explain_swap = MagicMock()

    # Act
    menu_system._view_swaps_for_counterparty(1)

    # Assert
    menu_system._explain_swap.assert_called_once_with('c1')


@patch('gamecock.menu_system.Console.print')
def test_explain_swap_success(mock_print, menu_system):
    """Test successfully explaining a swap."""
    # Arrange
    menu_system.swaps_analyzer.explain_swap.return_value = 'Swap explanation.'

    # Act
    menu_system._explain_swap('c1')

    # Assert
    mock_print.assert_any_call('Swap explanation.')


@patch('gamecock.menu_system.Prompt.ask')
@patch('gamecock.menu_system.input')
@patch('gamecock.menu_system.Console.print')
def test_generate_risk_report_with_detailed_analysis(mock_print, mock_input, mock_ask, menu_system):
    """Test printing of detailed analysis tables in risk report."""
    # Arrange
    menu_system.swaps_analyzer.generate_risk_report.return_value = {
        'risk_level': 'High',
        'risk_score': 80,
        'total_notional': 1000.0,
        'num_swaps': 2,
        'avg_time_to_maturity': 1.5,
        'detailed_analysis': {
            'counterparty_concentration': {'breakdown': {'CP1': 700.0, 'CP2': 300.0}},
            'currency_concentration': {'breakdown': {'USD': 1000.0}},
        },
        'ai_summary': 'summary',
    }

    # Provide entity name and avoid stdin read
    mock_ask.return_value = 'ENTITY'
    mock_input.return_value = ''
    # Act
    menu_system._generate_risk_report()

    # Assert: ensure Table objects were printed (summary + 2 breakdown tables)
    from rich.table import Table
    printed_tables = []
    for ca in mock_print.call_args_list:
        if ca and ca.args:
            first = ca.args[0]
            if isinstance(first, Table):
                printed_tables.append(first)
    # At least 3 tables: summary and two breakdown tables
    assert len(printed_tables) >= 3


@patch('gamecock.menu_system.input')
@patch('gamecock.menu_system.Prompt.ask')
def test_ai_analyst_menu_prompt_download_yes_then_analysis(mock_ask, mock_input, menu_system):
    """Covers prompt_download branch then continue to analysis on success."""
    menu_system.ai_analyst.ollama.is_running.return_value = True
    # First ask is for question, second is y/n to download
    mock_ask.side_effect = ['What about ABC', 'y']

    # First answer asks to download, second yields analysis
    menu_system.ai_analyst.answer.side_effect = [
        {'type': 'prompt_download', 'entity_name': 'ABC', 'message': 'Download?'},
        {'type': 'analysis', 'message': 'All good'},
    ]
    menu_system._download_data_for_entity = MagicMock(return_value=True)

    menu_system._ai_analyst_menu()

    menu_system._download_data_for_entity.assert_called_once_with('ABC')


@patch('gamecock.menu_system.input')
@patch('gamecock.menu_system.Prompt.ask')
def test_ai_analyst_menu_prompt_download_no(mock_ask, mock_input, menu_system):
    menu_system.ai_analyst.ollama.is_running.return_value = True
    mock_ask.side_effect = ['What about XYZ', 'n']
    menu_system.ai_analyst.answer.return_value = {'type': 'prompt_download', 'entity_name': 'XYZ', 'message': 'Download?'}

    menu_system._ai_analyst_menu()


@patch('gamecock.menu_system.input')
@patch('gamecock.menu_system.Prompt.ask')
def test_ai_analyst_menu_prompt_confirm_entity_yes(mock_ask, mock_input, menu_system):
    menu_system.ai_analyst.ollama.is_running.return_value = True
    mock_ask.side_effect = ['Analyze CP1', 'y']
    suggestion = {'type': 'counterparty', 'name': 'CP1', 'id': 1}
    menu_system.ai_analyst.answer.return_value = {'type': 'prompt_confirm_entity', 'suggestion': suggestion, 'message': 'Use CP1?'}
    menu_system._run_analysis_for_entity = MagicMock()

    menu_system._ai_analyst_menu()

    menu_system._run_analysis_for_entity.assert_called_once_with('Analyze CP1', suggestion)


@patch('gamecock.menu_system.input')
@patch('gamecock.menu_system.Console.print')
def test_view_loaded_swaps_truncation_note(mock_print, mock_input, menu_system):
    """When >50 swaps are loaded, show truncation note."""
    # Build 55 mock swaps with required attributes
    swaps = []
    for i in range(55):
        s = MagicMock()
        s.contract_id = f"C{i}"
        s.reference_entity = "ENT"
        s.notional_amount = 1_000.0
        s.swap_type.value = 'CDS'
        s.maturity_date.isoformat.return_value = '2025-01-01'
        swaps.append(s)
    menu_system.swaps_analyzer.swaps = swaps

    menu_system._view_loaded_swaps()

    # Look for the truncation note
    printed = [args[0] for args, _ in [ (c[0], c[1]) if len(c) == 2 else (c, {}) for c in [call.args for call in mock_print.mock_calls] ] if args]
    assert any('(Showing 50 of 55 swaps)' in str(p) for p in printed)


@patch('gamecock.menu_system.Prompt.ask')
@patch('gamecock.menu_system.input')
def test_file_browser_handles_file_not_found_then_quit(mock_input, mock_ask, menu_system):
    """Ensure _file_browser recovers from FileNotFoundError and allows quitting."""
    # Mock a path-like object
    start_path = MagicMock()
    start_path.resolve.return_value = start_path
    start_path.parent = start_path  # just loop to itself for parent

    # First iterdir raises, then returns empty list
    seq = [FileNotFoundError(), []]
    def iterdir_side_effect():
        v = seq.pop(0)
        if isinstance(v, Exception):
            raise v
        return v
    start_path.iterdir.side_effect = iterdir_side_effect

    mock_ask.side_effect = ['q']  # after recovery, quit

    res = menu_system._file_browser(start_path)
    assert res is None
@patch('gamecock.menu_system.Console.print')
@patch('gamecock.menu_system.Prompt.ask')
def test_download_filings_for_company_no_files_found(mock_ask, mock_print, menu_system):
    """Test downloading filings when no files are found for the parent company."""
    # Arrange
    mock_company_info = MagicMock()
    mock_company_info.primary_identifiers.cik = '12345'
    mock_company_info.name = 'TestCo'
    mock_company_info.related_entities = []
    mock_ask.side_effect = ['2']
    menu_system.downloader.download_company_filings.return_value = []  # No files downloaded

    # Act
    menu_system._download_filings_for_company(mock_company_info)

    # Assert
    mock_print.assert_any_call('No filings were downloaded. Please try again or check the company information.')


@patch('gamecock.menu_system.Console.print')
@patch('gamecock.menu_system.Prompt.ask')
def test_download_filings_for_company_no_related_files(mock_ask, mock_print, menu_system):
    """Test downloading filings when no files are found for a related entity."""
    # Arrange
    mock_parent_company = MagicMock()
    mock_parent_company.primary_identifiers.cik = '12345'
    mock_parent_company.name = 'ParentCo'
    mock_related_entity = MagicMock()
    mock_related_entity.cik = '67890'
    mock_related_entity.name = 'ChildCo'
    mock_parent_company.related_entities = [mock_related_entity]
    mock_ask.side_effect = ['1']  # Download all
    menu_system.downloader.download_company_filings.side_effect = [
        ['parent_file.txt'],  # Parent has files
        []                    # Related entity has no files
    ]

    # Act
    menu_system._download_filings_for_company(mock_parent_company)

    # Assert
    mock_print.assert_any_call('No filings found for ChildCo')


@patch('gamecock.menu_system.input')
@patch('gamecock.menu_system.Console.print')
def test_view_companies_menu_with_related_entities(mock_print, mock_input, menu_system):
    """Test viewing companies when a company has related entities."""
    # Arrange
    mock_related = MagicMock()
    mock_related.name = 'Related Co'
    mock_related.cik = '54321'
    mock_company = MagicMock()
    mock_company.name = 'Test Company'
    mock_company.primary_identifiers.cik = '12345'
    mock_company.primary_identifiers.description = 'A test company.'
    mock_company.related_entities = [mock_related]
    menu_system.db.get_all_companies.return_value = [mock_company]

    # Act
    menu_system.view_companies_menu()

    # Assert
    mock_print.assert_any_call('- Related Co (CIK: 54321)')


@patch('gamecock.menu_system.input')
@patch('gamecock.menu_system.Console.print')
def test_list_all_reference_securities_no_data(mock_print, mock_input, menu_system):
    """Test listing reference securities when none are in the database."""
    # Arrange
    menu_system.db.get_all_reference_securities.return_value = []

    # Act
    menu_system._list_all_reference_securities()

    # Assert
    mock_print.assert_any_call('[yellow]No reference securities found in the database.[/yellow]')


@patch('gamecock.menu_system.Prompt.ask')
def test_list_all_reference_securities_with_data(mock_ask, menu_system):
    """Test listing reference securities and selecting one."""
    # Arrange
    securities = [{'id': 1, 'identifier': 'SEC1', 'security_type': 'Equity', 'description': 'Test Sec'}]
    menu_system.db.get_all_reference_securities.return_value = securities
    mock_ask.return_value = '1'
    menu_system._view_swaps_for_security = MagicMock()

    # Act
    menu_system._list_all_reference_securities()

    # Assert
    menu_system._view_swaps_for_security.assert_called_once_with(1)


@patch('gamecock.menu_system.input')
@patch('gamecock.menu_system.Console.print')
def test_view_swaps_for_security_no_swaps(mock_print, mock_input, menu_system):
    """Test viewing swaps for a security that has no swaps."""
    # Arrange
    menu_system.db.get_swaps_by_security_id.return_value = []

    # Act
    menu_system._view_swaps_for_security(1)

    # Assert
    mock_print.assert_any_call('[yellow]No swaps found for security ID 1.[/yellow]')


@patch('gamecock.menu_system.Prompt.ask')
@patch('gamecock.menu_system.input')
def test_view_swaps_for_security_with_swaps(mock_input, mock_ask, menu_system):
    """Test viewing swaps for a security and explaining one."""
    # Arrange
    swaps = [{'contract_id': 'c1', 'counterparty': 'CP1', 'currency': 'USD', 'notional_amount': 100, 'maturity_date': '2023-01-01'}]
    menu_system.db.get_swaps_by_security_id.return_value = swaps
    mock_ask.return_value = 'c1'
    menu_system._explain_swap = MagicMock()

    # Act
    menu_system._view_swaps_for_security(1)

    # Assert
    menu_system._explain_swap.assert_called_once_with('c1')


@patch('gamecock.menu_system.Prompt.ask')
@patch('gamecock.menu_system.input')
def test_reimport_data_menu_success(mock_input, mock_ask, menu_system):
    """Test the re-import menu with user confirmation."""
    # Arrange
    mock_data_dir = MagicMock()
    mock_data_dir.exists.return_value = True
    mock_data_dir.iterdir.return_value = [MagicMock()]  # Non-empty
    mock_ask.return_value = 'y'
    menu_system.swaps_processor.process_directory = MagicMock()

    # Act
    menu_system._reimport_data_menu(data_dir=mock_data_dir)

    # Assert
    menu_system.swaps_processor.process_directory.assert_called_once_with(mock_data_dir, save_to_db=True)


@patch('gamecock.menu_system.input')
def test_ai_analyst_menu_exit(mock_input, menu_system):
    """Test AI analyst menu exits early when Ollama is not running (no prompt)."""
    # Arrange
    menu_system.ai_analyst.ollama.is_running.return_value = False
    menu_system.ai_analyst.search_for_entity = MagicMock()

    # Act
    menu_system._ai_analyst_menu()

    # Assert
    menu_system.ai_analyst.search_for_entity.assert_not_called()


@patch('gamecock.menu_system.input')
def test_ai_analyst_menu_ollama_not_running(mock_input, menu_system):
    """Test AI analyst menu exits early when Ollama is not running."""
    # Arrange
    menu_system.ai_analyst.ollama.is_running.return_value = False

    # Act
    menu_system._ai_analyst_menu()

    # Assert
    menu_system.ai_analyst.answer.assert_not_called()


@patch('gamecock.menu_system.input')
@patch('gamecock.menu_system.Prompt.ask')
def test_ai_analyst_menu_user_backs_out_immediately(mock_ask, mock_input, menu_system):
    """Test AI analyst menu when user provides empty question (back)."""
    # Arrange
    menu_system.ai_analyst.ollama.is_running.return_value = True
    mock_ask.return_value = ''  # User presses Enter to go back

    # Act
    menu_system._ai_analyst_menu()

    # Assert
    menu_system.ai_analyst.answer.assert_not_called()





