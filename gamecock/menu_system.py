"""Menu system for SEC data handler."""
from rich.console import Console
from rich.prompt import Prompt
from rich.status import Status
from rich.table import Table
from datetime import datetime, timedelta
from loguru import logger
import sys

# Configure logger to show debug messages
logger.remove()
logger.add(sys.stderr, level="DEBUG")

from .data_structures import CompanyInfo, EntityIdentifiers
from .db_handler import DatabaseHandler
from .sec_handler import SECHandler
from .downloader import SECDownloader

console = Console()

def print_ascii_art():
    """Display the Gamecock ASCII art."""
    console.print(r"""
                                                  __    
   _________    _____   ____   ____  ____   ____ |  | __
  / ___\__  \  /     \_/ __ \_/ ___\/  _ \_/ ___\|  |/ /
 / /_/  > __ \|  Y Y  \  ___/\  \__(  <_> )  \___|    < 
 \___  (____  /__|_|  /\___  >\___  >____/ \___  >__|_ |
/_____/     \/      \/     \/     \/           \/     \|
    """, style="bold blue")

class MenuSystem:
    """Menu system for SEC filing downloader."""
    
    def __init__(self):
        """Initialize menu system."""
        self.console = Console()
        self.db = DatabaseHandler()
        self.sec = SECHandler()

    def main_menu(self):
        while True:
            self.console.clear()
            print_ascii_art()
            self.console.print("\nOptions:")
            self.console.print("1. Search for Company")
            self.console.print("2. View Saved Companies")
            self.console.print("3. Download Filings")
            self.console.print("4. View Downloaded Data")
            self.console.print("5. Exit")
            
            choice = Prompt.ask("\nSelect an option", choices=["1", "2", "3", "4", "5"])
            
            if choice == "1":
                self.search_company_menu()
            elif choice == "2":
                self.view_companies_menu()
            elif choice == "3":
                self.download_filings_menu()
            elif choice == "4":
                self.view_data_menu()
            elif choice == "5":
                break

    def search_company_menu(self):
        self.console.clear()
        print_ascii_art()
        self.console.print("\n[bold blue]Company Search[/bold blue]\n")
        
        company_name = Prompt.ask("\nEnter company name")
        if not company_name:
            return
            
        company_info = self.identify_company(company_name)
            
        if company_info:
            self.console.print("\n[green]Company found![/green]")
            
            choice = self.display_company_info(company_info)
            
            if choice != 0:
                entity = company_info.related_entities[choice - 1]
                self.display_company_info(entity)
            
            if Prompt.ask("\nSave this company?", choices=["y", "n"]) == "y":
                if self.db.save_company(company_info):
                    self.console.print("[green]Company saved successfully![/green]")
                else:
                    self.console.print("[red]Failed to save company.[/red]")
                
            # Offer to download filings
            if Prompt.ask("\nDownload filings for this company?", choices=["y", "n"]) == "y":
                self._download_filings_for_company(company_info)
        else:
            self.console.print("[red]Could not identify company.[/red]")
            self.console.print("Try:")
            self.console.print("1. Using the full official company name")
            self.console.print("2. Including 'Corporation', 'Inc.', etc.")
            self.console.print("3. Adding the stock ticker if known")
            
        input("\nPress Enter to continue...")

    def identify_company(self, query: str) -> CompanyInfo:
        """Identify company from user query."""
        try:
            # Search SEC EDGAR
            company = self.sec.get_company_info(query)
            if not company:
                return None
            return company
            
        except Exception as e:
            logger.error(f"Error identifying company: {str(e)}")
            return None

    def display_company_info(self, company: CompanyInfo):
        """Display company information."""
        if not company:
            return
            
        self.console.print("\n[bold cyan]Primary Company Information[/bold cyan]")
        
        # Display primary company info in a table
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Identifier")
        table.add_column("Value")
        
        table.add_row("Name", company.name)
        if company.primary_identifiers:
            if company.primary_identifiers.cik:
                table.add_row("CIK", company.primary_identifiers.cik)
            if company.primary_identifiers.tickers:
                tickers = []
                for ticker in company.primary_identifiers.tickers:
                    if isinstance(ticker, dict):
                        ticker_str = ticker.get('symbol', '')
                        if ticker.get('exchange'):
                            ticker_str += f" ({ticker['exchange']})"
                        tickers.append(ticker_str)
                    else:
                        tickers.append(str(ticker))
                table.add_row("Tickers", ", ".join(tickers))
            if company.primary_identifiers.description:
                table.add_row("Description", company.primary_identifiers.description)
                
        self.console.print(table)
        
        # Display related entities
        if company.related_entities:
            self.console.print("\n[bold cyan]Related Entities[/bold cyan]")
            for i, entity in enumerate(company.related_entities, 1):
                self.console.print(f"\n{i}. {entity.name}")
                if entity.relationship:
                    self.console.print(f"   Relationship: {entity.relationship}")
                if entity.cik:
                    self.console.print(f"   CIK: {entity.cik}")
                if entity.tickers:
                    tickers = []
                    for ticker in entity.tickers:
                        if isinstance(ticker, dict):
                            ticker_str = ticker.get('symbol', '')
                            if ticker.get('exchange'):
                                ticker_str += f" ({ticker['exchange']})"
                            tickers.append(ticker_str)
                        else:
                            tickers.append(str(ticker))
                    self.console.print(f"   Tickers: {', '.join(tickers)}")
                if entity.description:
                    self.console.print(f"   Description: {entity.description}")
                    
        # Prompt for related entity details
        if company.related_entities:
            self.console.print("\n[dim]Enter 0 to proceed with primary company, or select a number to view related entity details[/dim]")
            choice = Prompt.ask("Select entity", choices=["0"] + [str(i) for i in range(1, len(company.related_entities) + 1)])
            return int(choice)
        return 0

    def _download_filings_for_company(self, company: CompanyInfo):
        """Handle filing downloads for a company."""
        self.console.print("\n[bold]Download Options[/bold]")
        
        options = {
            "1": "Download all filings (parent and related entities)",
            "2": "Download parent company filings only",
            "3": "Return to main menu"
        }
        
        for key, label in options.items():
            self.console.print(f"{key}. {label}")
            
        choice = Prompt.ask("Choose option", choices=list(options.keys()))
        
        if choice == "3":
            return
            
        # Set up time range - default to last 10 years
        end = datetime.now()
        start = end - timedelta(days=365*10)
        
        # Initialize downloader
        downloader = SECDownloader()
        
        # Determine which entities to download
        entities_to_download = []
        if choice == "1":
            entities_to_download = [company.primary_identifiers] + company.related_entities
        else:  # choice == "2"
            entities_to_download = [company.primary_identifiers]
            
        # Common filing types
        filing_types = ["10-K", "10-Q", "8-K", "13F", "4", "SC 13G"]
        
        # Download filings for each selected entity
        total_downloaded = 0
        for entity in entities_to_download:
            if not entity.cik:
                self.console.print(f"[yellow]Skipping {entity.name} - No CIK available[/yellow]")
                continue
                
            self.console.print(f"\n[bold]Downloading filings for {entity.name}[/bold]")
            try:
                downloaded = downloader.download_company_filings(
                    entity.cik,
                    start,
                    end,
                    filing_types
                )
                total_downloaded += len(downloaded)
                
                # Show download summary
                self.console.print(f"[green]Successfully downloaded {len(downloaded)} filings for {entity.name}[/green]")
                
            except Exception as e:
                self.console.print(f"[red]Error downloading filings for {entity.name}: {str(e)}[/red]")
                continue
                
        if total_downloaded > 0:
            self.console.print(f"\n[bold green]Download complete! Total filings downloaded: {total_downloaded}[/bold green]")
            self.console.print("Files are saved in the data/filings directory, organized by CIK and accession number.")
        else:
            self.console.print("\n[yellow]No filings were downloaded. Please try again or check the company information.[/yellow]")

    def view_companies_menu(self):
        """Display and manage company information."""
        self.console.clear()
        
        # Get company info from user
        company_name = Prompt.ask("\nEnter company name or identifier")
        
        try:
            # Get company information
            company = self.identify_company(company_name)
            if not company:
                self.console.print("[red]Could not identify company. Please try again.[/red]")
                return
                
            # Display company information
            choice = self.display_company_info(company)
            
            if choice != 0:
                entity = company.related_entities[choice - 1]
                self.display_company_info(entity)
            
            # Ask to save company
            if Prompt.ask("Save this company?", choices=["y", "n"]) == "y":
                if self.db.save_company(company):
                    self.console.print("[green]Company saved successfully![/green]")
                else:
                    self.console.print("[red]Failed to save company.[/red]")
            
            # Download menu
            self.console.print("\n[bold]Download Options[/bold]")
            options = {
                "0": "Return to main menu",
                "1": "Download all filings (parent and related entities)",
                "2": "Download parent company filings only"
            }
            
            for key, label in options.items():
                self.console.print(f"{key}. {label}")
                
            choice = Prompt.ask("Choose option", choices=list(options.keys()))
            
            if choice == "0":
                return
                
            # Handle download based on choice
            if choice in ["1", "2"]:
                try:
                    if not company.primary_identifiers.cik:
                        self.console.print("[red]Error: No CIK available for this company[/red]")
                        return
                        
                    # Initialize downloader
                    downloader = SECDownloader()
                    
                    # Set date range - default to last 10 years
                    end_date = datetime.now()
                    start_date = end_date - timedelta(days=365*10)
                    
                    # Common filing types
                    filing_types = ["10-K", "10-Q", "8-K", "13F", "4", "SC 13G"]
                    
                    # Download for primary company
                    self.console.print(f"\n[bold]Downloading filings for {company.name}[/bold]")
                    
                    # Download for selected entities
                    if choice == "1":
                        entities = [company.primary_identifiers] + company.related_entities
                    else:
                        entities = [company.primary_identifiers]
                        
                    total_downloaded = 0
                    for entity in entities:
                        if not entity.cik:
                            self.console.print(f"[yellow]Skipping {entity.name} - No CIK available[/yellow]")
                            continue
                            
                        try:
                            downloaded = downloader.download_company_filings(
                                entity.cik,
                                start_date,
                                end_date,
                                filing_types
                            )
                            count = len(downloaded)
                            total_downloaded += count
                            self.console.print(f"[green]Successfully downloaded {count} filings for {entity.name}[/green]")
                            
                        except Exception as e:
                            self.console.print(f"[red]Error downloading filings for {entity.name}: {str(e)}[/red]")
                            continue
                            
                    if total_downloaded > 0:
                        self.console.print(f"\n[bold green]Download complete! Total filings downloaded: {total_downloaded}[/bold green]")
                        self.console.print("Files are saved in the data/filings directory, organized by CIK and accession number.")
                    else:
                        self.console.print("\n[yellow]No filings were downloaded. Please try again or check the company information.[/yellow]")
                        
                except Exception as e:
                    self.console.print(f"[red]Error during download process: {str(e)}[/red]")
                    
        except Exception as e:
            self.console.print(f"[red]Error: {str(e)}[/red]")

    def download_filings_menu(self):
        self.console.clear()
        print_ascii_art()
        self.console.print("\n[bold blue]Download Filings[/bold blue]\n")
        
        companies = self.db.get_all_companies()
        if not companies:
            self.console.print("[yellow]No companies saved. Please search and save a company first.[/yellow]")
        else:
            self.console.print("Select a company to download filings for:")
            for i, company in enumerate(companies, 1):
                self.console.print(f"{i}. {company.name}")
            
            choice = Prompt.ask(
                "\nSelect company (0 to return)",
                choices=["0"] + [str(i) for i in range(1, len(companies) + 1)]
            )
            
            if choice != "0":
                company = companies[int(choice) - 1]
                self._download_filings_for_company(company)
        
        input("\nPress Enter to continue...")

    def view_data_menu(self):
        self.console.clear()
        print_ascii_art()
        self.console.print("\n[bold blue]View Data[/bold blue]\n")
        self.console.print("Feature coming soon!")
        input("\nPress Enter to continue...")
