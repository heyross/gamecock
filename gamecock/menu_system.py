"""Menu system for SEC data handler."""
from rich.console import Console
from rich.prompt import Prompt
from typing import Optional, Dict
from rich.status import Status
from rich.table import Table
from datetime import datetime, timedelta
from loguru import logger
import sys
from pathlib import Path
import json

# Configure logger to show debug messages
logger.remove()
logger.add(sys.stderr, level="DEBUG")

from .data_structures import CompanyInfo, EntityIdentifiers
from .db_handler import DatabaseHandler
from .sec_handler import SECHandler
from .downloader import SECDownloader
from .swaps_analyzer import SwapsAnalyzer
from .ai_analyst import AIAnalyst

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
        self.swaps_analyzer = SwapsAnalyzer(db_handler=self.db)
        self.ai_analyst = AIAnalyst(db_handler=self.db)

    def main_menu(self):
        """Run the main menu system."""
        while True:
            self.console.clear()
            print_ascii_art()
            self.console.print("\n[bold blue]Main Menu[/bold blue]\n")
            self.console.print("1. Search for Company")
            self.console.print("2. View Saved Companies")
            self.console.print("3. Download Filings")
            self.console.print("4. View Downloaded Data")
            self.console.print("5. Swaps Analysis")
            self.console.print("6. Data Explorer")
            self.console.print("7. AI Analyst")
            self.console.print("8. Exit")
            
            choice = Prompt.ask("\nSelect an option", choices=["1", "2", "3", "4", "5", "6", "7", "8"])
            
            if choice == "1":
                self.search_company_menu()
            elif choice == "2":
                self.view_companies_menu()
            elif choice == "3":
                self.download_filings_menu()
            elif choice == "4":
                self.view_data_menu()
            elif choice == "5":
                self.swaps_analysis_menu()
            elif choice == "6":
                self.data_explorer_menu()
            elif choice == "7":
                self._ai_analyst_menu()
            elif choice == "8":
                break
                
    def search_company_menu(self):
        """Menu for searching companies."""
        self.console.clear()
        print_ascii_art()
        self.console.print("\n[bold blue]Company Search[/bold blue]\n")
        
        company_name = Prompt.ask("Enter company name or ticker (or press Enter to go back)")
        if not company_name:
            return
            
        company_info = self.sec.get_company_info(company_name)
        if company_info:
            self.console.print("\n[green]Company found![/green]")
            self.display_company_info(company_info)
            
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
        
    def display_company_info(self, company_info):
        """Display company information."""
        self.console.print("\n[bold]Company Information[/bold]")
        self.console.print(f"Name: {company_info.name}")
        self.console.print(f"CIK: {company_info.primary_identifiers.cik}")
        if company_info.primary_identifiers.tickers:
            ticker_info = company_info.primary_identifiers.tickers[0]
            if isinstance(ticker_info, dict):
                ticker = ticker_info['symbol']
                exchange = ticker_info.get('exchange', 'Unknown')
                self.console.print(f"Ticker: {ticker} ({exchange})")
            else:
                self.console.print(f"Ticker: {ticker_info}")
        
        if company_info.related_entities:
            self.console.print("\n[bold]Related Entities[/bold]")
            for entity in company_info.related_entities:
                self.console.print(f"- {entity.name} (CIK: {entity.cik})")
                
    def _download_filings_for_company(self, company_info):
        """Handle filing downloads for a company."""
        self.console.print("\n[bold]Download Options[/bold]")
        self.console.print("1. Download all filings (parent and related entities)")
        self.console.print("2. Download parent company filings only")
        self.console.print("3. Return to main menu")
        
        choice = Prompt.ask("Choose option", choices=["1", "2", "3"])
        if choice == "3":
            return
            
        # Set date range
        end = datetime.now()
        start = end - timedelta(days=365)  # Default to 1 year
        
        try:
            self.console.print(f"\nDownloading filings for {company_info.name}")
            
            # Initialize downloader with shared handlers
            downloader = SECDownloader(db_handler=self.db, swaps_analyzer=self.swaps_analyzer)
            
            # Download parent company filings
            downloaded_files = downloader.download_company_filings(
                company_info.primary_identifiers.cik,
                start,
                end
            )
            
            if downloaded_files:
                self.console.print(f"Successfully downloaded {len(downloaded_files)} filings")
            else:
                self.console.print("No filings were downloaded. Please try again or check the company information.")
                
            # Download related entity filings if requested
            if choice == "1" and company_info.related_entities:
                for entity in company_info.related_entities:
                    if entity.cik:
                        self.console.print(f"\nDownloading filings for related entity: {entity.name}")
                        related_files = downloader.download_company_filings(
                            entity.cik,
                            start,
                            end
                        )
                        if related_files:
                            self.console.print(f"Successfully downloaded {len(related_files)} filings")
                        else:
                            self.console.print(f"No filings found for {entity.name}")
                            
        except ValueError as e:
            self.console.print(f"[red]Configuration Error: {e}[/red]")
        except ConnectionError as e:
            self.console.print(f"[red]Network Error: Could not connect to SEC EDGAR. Please check your internet connection. Details: {e}[/red]")
        except Exception as e:
            self.console.print(f"[red]An unexpected error occurred during download: {e}[/red]")
            
    def download_filings_menu(self):
        """Menu for downloading filings."""
        self.console.clear()
        print_ascii_art()
        self.console.print("\n[bold blue]Download Filings[/bold blue]\n")
        
        companies = self.db.get_all_companies()
        if not companies:
            self.console.print("[yellow]No companies saved. Please search and save a company first.[/yellow]")
            input("\nPress Enter to continue...")
            return
            
        self.console.print("Saved Companies:")
        for i, company in enumerate(companies, 1):
            self.console.print(f"{i}. {company.name}")
        self.console.print("0. Return to main menu")
        
        choice = Prompt.ask("\nSelect a company", choices=[str(i) for i in range(len(companies) + 1)])
        
        if choice != "0":
            company = companies[int(choice) - 1]
            self._download_filings_for_company(company)
            
        input("\nPress Enter to continue...")
        
    def view_companies_menu(self):
        """Menu for viewing saved companies."""
        self.console.clear()
        print_ascii_art()
        self.console.print("\n[bold blue]Saved Companies[/bold blue]\n")
        
        companies = self.db.get_all_companies()
        if not companies:
            self.console.print("[yellow]No companies saved.[/yellow]")
        else:
            for company in companies:
                self.console.print(f"\n[bold]{company.name}[/bold]")
                self.console.print(f"CIK: {company.primary_identifiers.cik}")
                if company.primary_identifiers.description:
                    self.console.print(f"Description: {company.primary_identifiers.description}")
                    
                # Show related entities
                related = company.related_entities
                if related:
                    self.console.print("\nRelated Entities:")
                    for entity in related:
                        self.console.print(f"- {entity.name} (CIK: {entity.cik})")
                        
        input("\nPress Enter to continue...")
        
    def view_data_menu(self):
        """Menu for viewing downloaded data."""
        self.console.clear()
        print_ascii_art()
        self.console.print("\n[bold blue]View Downloaded Data[/bold blue]\n")
        
        try:
            # Get database statistics
            cursor = self.db.cursor
            
            # Total number of filings
            cursor.execute("SELECT COUNT(*) FROM filings")
            total_filings = cursor.fetchone()[0]
            
            # Number of companies
            cursor.execute("SELECT COUNT(DISTINCT company_cik) FROM filings")
            total_companies = cursor.fetchone()[0]
            
            # Filing types breakdown
            cursor.execute("""
                SELECT form_type, COUNT(*) as count 
                FROM filings 
                GROUP BY form_type 
                ORDER BY count DESC
            """)
            filing_types = cursor.fetchall()
            
            # Latest filing date
            cursor.execute("SELECT MAX(filing_date) FROM filings")
            latest_filing = cursor.fetchone()[0]
            
            # Create statistics table
            table = Table(title="Filing Statistics")
            table.add_column("Metric", style="cyan")
            table.add_column("Value", style="green")
            
            table.add_row("Total Filings", str(total_filings))
            table.add_row("Total Companies", str(total_companies))
            table.add_row("Latest Filing Date", str(latest_filing))
            
            self.console.print(table)
            
            # Create filing types table
            if filing_types:
                types_table = Table(title="\nFiling Types Breakdown")
                types_table.add_column("Form Type", style="cyan")
                types_table.add_column("Count", style="green")
                
                for form_type, count in filing_types:
                    types_table.add_row(form_type or "Unknown", str(count))
                    
                self.console.print(types_table)
            
        except Exception as e:
            self.console.print(f"[red]Error getting statistics: {str(e)}[/red]")
            
        input("\nPress Enter to continue...")
        
    def swaps_analysis_menu(self):
        """Menu for swaps analysis functionality."""
        while True:
            self.console.clear()
            print_ascii_art()
            self.console.print("\n[bold blue]Swaps Analysis[/bold blue]\n")
            
            self.console.print("1. Load Swaps from File")
            self.console.print("2. View Loaded Swaps")
            self.console.print("3. Analyze Reference Entity Exposure")
            self.console.print("4. Generate Risk Report")
            self.console.print("5. Export Swaps Data")
            self.console.print("6. Back to Main Menu")
            
            choice = Prompt.ask("\nSelect an option", choices=["1", "2", "3", "4", "5", "6"])
            
            if choice == "1":
                self._load_swaps_from_file()
            elif choice == "2":
                self._view_loaded_swaps()
            elif choice == "3":
                self._analyze_entity_exposure()
            elif choice == "4":
                self._generate_risk_report()
            elif choice == "5":
                self._export_swaps_data()
            elif choice == "6":
                break
    
    def _load_swaps_from_file(self):
        """Load swaps data from a file."""
        self.console.clear()
        print_ascii_art()
        self.console.print("\n[bold blue]Load Swaps from File[/bold blue]\n")
        
        start_path = Path.cwd() / 'data'
        start_path.mkdir(exist_ok=True)
        file_path = self._file_browser(start_path=start_path)

        if not file_path:
            return
            
        try:
            with self.console.status("[bold green]Loading swaps data...[/]"):
                swaps = self.swaps_analyzer.load_swaps_from_file(file_path)
                if swaps:
                    self.console.print(f"\n[green]Successfully loaded {len(swaps)} swaps.[/green]")
                else:
                    self.console.print("[yellow]No valid swaps found in the file.[/yellow]")
        except FileNotFoundError:
            self.console.print(f"[red]Error: The file was not found at the specified path.[/red]")
        except json.JSONDecodeError:
            self.console.print("[red]Error: The JSON file is malformed and could not be parsed.[/red]")
        except Exception as e:
            self.console.print(f"[red]An unexpected error occurred while loading the file: {e}[/red]")
            
        input("\nPress Enter to continue...")
    
    def _view_loaded_swaps(self):
        """View currently loaded swaps."""
        self.console.clear()
        print_ascii_art()
        self.console.print("\n[bold blue]Loaded Swaps[/bold blue]\n")
        
        swaps = self.swaps_analyzer.swaps
        if not swaps:
            self.console.print("[yellow]No swaps loaded.[/yellow]")
            input("\nPress Enter to continue...")
            return
            
        # Create a table to display swaps
        table = Table(title=f"Loaded Swaps ({len(swaps)} total)")
        table.add_column("Contract ID", style="cyan")
        table.add_column("Reference Entity", style="magenta")
        table.add_column("Notional", style="green")
        table.add_column("Type", style="yellow")
        table.add_column("Maturity", style="blue")
        
        for swap in swaps[:50]:  # Show first 50 swaps to avoid overwhelming the console
            table.add_row(
                swap.contract_id,
                swap.reference_entity,
                f"${swap.notional_amount:,.2f}",
                swap.swap_type.value if hasattr(swap.swap_type, 'value') else swap.swap_type,
                swap.maturity_date.isoformat() if hasattr(swap.maturity_date, 'isoformat') else str(swap.maturity_date)
            )
            
        self.console.print(table)
        
        if len(swaps) > 50:
            self.console.print(f"\n[dim](Showing 50 of {len(swaps)} swaps)[/dim]")
            
        input("\nPress Enter to continue...")
    
    def _analyze_entity_exposure(self):
        """Analyze exposure to a reference entity."""
        self.console.clear()
        print_ascii_art()
        self.console.print("\n[bold blue]Analyze Reference Entity Exposure[/bold blue]\n")
        
        entity_name = Prompt.ask("Enter reference entity name")
        if not entity_name:
            return
            
        try:
            with self.console.status(f"[bold green]Analyzing exposure to {entity_name}...[/]"):
                exposure = self.swaps_analyzer.calculate_exposure(entity_name)
                
                if not exposure:
                    self.console.print("[yellow]No exposure found for the specified entity.[/yellow]")
                    input("\nPress Enter to continue...")
                    return
                
                # Display exposure summary
                self.console.print("\n[bold]Exposure Summary:[/bold]")
                table = Table(show_header=False, show_edge=False)
                table.add_column(style="cyan")
                table.add_column(style="green")
                
                table.add_row("Total Notional:", f"${exposure['total_notional']:,.2f}")
                table.add_row("Number of Swaps:", str(exposure['num_swaps']))
                table.add_row("Average Notional:", f"${exposure['avg_notional']:,.2f}")
                table.add_row("Largest Swap:", f"${exposure['largest_swap']['notional_amount']:,.2f}" + 
                             f" (ID: {exposure['largest_swap']['contract_id']})")
                
                self.console.print(table)
                
        except Exception as e:
            self.console.print(f"[red]Error analyzing exposure: {str(e)}[/red]")
            
        input("\nPress Enter to continue...")
    
    def _generate_risk_report(self):
        """Generate a risk report for a reference entity."""
        self.console.clear()
        print_ascii_art()
        self.console.print("\n[bold blue]Generate Risk Report[/bold blue]\n")
        
        entity_name = Prompt.ask("Enter reference entity name")
        if not entity_name:
            return
            
        try:
            with self.console.status(f"[bold green]Generating risk report for {entity_name}...[/]"):
                report = self.swaps_analyzer.generate_risk_report(entity_name, include_analysis=True)
                
                if not report:
                    self.console.print("[yellow]No data found for the specified entity.[/yellow]")
                    input("\nPress Enter to continue...")
                    return
                
                # Display risk summary
                self.console.print("\n[bold]Risk Report[/bold]")
                summary_table = Table(show_header=False, show_edge=False)
                summary_table.add_column(style="cyan")
                summary_table.add_column(style="green")

                risk_level = report.get('risk_level', 'Unknown')
                risk_score = report.get('risk_score', 0)

                if risk_level == "Low":
                    risk_display = f"[green]{risk_level} ({risk_score:.2f}/100)[/green]"
                elif risk_level == "Medium":
                    risk_display = f"[yellow]{risk_level} ({risk_score:.2f}/100)[/yellow]"
                else:
                    risk_display = f"[red]{risk_level} ({risk_score:.2f}/100)[/red]"

                summary_table.add_row("Risk Level:", risk_display)
                summary_table.add_row("Total Notional Exposure:", f"${report['total_notional']:,.2f}")
                summary_table.add_row("Number of Swaps:", str(report['num_swaps']))
                summary_table.add_row("Average Time to Maturity (Years):", f"{report['avg_time_to_maturity']:.2f}")
                self.console.print(summary_table)

                # Display detailed analysis
                if 'detailed_analysis' in report:
                    self.console.print("\n[bold]Detailed Risk Breakdown[/bold]")
                    details = report['detailed_analysis']

                    # Counterparty Risk
                    cp_table = Table(title="Counterparty Exposure")
                    cp_table.add_column("Counterparty", style="cyan")
                    cp_table.add_column("Notional", style="green")
                    cp_breakdown = details.get('counterparty_concentration', {}).get('breakdown', {})
                    for cp, amount in cp_breakdown.items():
                        cp_table.add_row(cp, f"${amount:,.2f}")
                    self.console.print(cp_table)

                    # Currency Risk
                    ccy_table = Table(title="Currency Exposure")
                    ccy_table.add_column("Currency", style="cyan")
                    ccy_table.add_column("Notional", style="green")
                    ccy_breakdown = details.get('currency_concentration', {}).get('breakdown', {})
                    for ccy, amount in ccy_breakdown.items():
                        ccy_table.add_row(ccy, f"${amount:,.2f}")
                    self.console.print(ccy_table)

                # Display AI Summary
                if 'ai_summary' in report:
                    self.console.print("\n[bold]AI Executive Summary[/bold]")
                    self.console.print(f"[italic]{report['ai_summary']}[/italic]")
                
        except Exception as e:
            self.console.print(f"[red]Error generating risk report: {str(e)}[/red]")
            
        input("\nPress Enter to continue...")
    
    def _export_swaps_data(self):
        """Export swaps data to a CSV file."""
        self.console.clear()
        print_ascii_art()
        self.console.print("\n[bold blue]Export Swaps Data[/bold blue]\n")
        
        if not self.swaps_analyzer.swaps:
            self.console.print("[yellow]No swaps loaded to export.[/yellow]")
            input("\nPress Enter to continue...")
            return
            
        output_path = Prompt.ask("Enter output file path (e.g., swaps_export.csv)")
        if not output_path:
            return
            
        try:
            if self.swaps_analyzer.export_to_csv(output_path):
                self.console.print(f"\n[green]Successfully exported swaps to {output_path}[/green]")
            else:
                self.console.print("[red]Failed to export swaps data.[/red]")
        except Exception as e:
            self.console.print(f"[red]Error exporting swaps: {str(e)}[/red]")
            
        input("\nPress Enter to continue...")
        
    def data_explorer_menu(self):
        """Menu for exploring the swaps database."""
        while True:
            self.console.clear()
            print_ascii_art()
            self.console.print("\n[bold blue]Data Explorer[/bold blue]\n")
            self.console.print("1. List All Counterparties")
            self.console.print("2. List All Reference Securities")
            self.console.print("3. Back to Main Menu")
            
            choice = Prompt.ask("\nSelect an option", choices=["1", "2", "3"])
            
            if choice == "1":
                self._list_all_counterparties()
            elif choice == "2":
                self._list_all_reference_securities()
            elif choice == "3":
                break

    def _list_all_counterparties(self):
        """List all counterparties and view their swaps."""
        self.console.clear()
        print_ascii_art()
        self.console.print("\n[bold blue]All Counterparties[/bold blue]\n")
        
        counterparties = self.db.get_all_counterparties()
        if not counterparties:
            self.console.print("[yellow]No counterparties found in the database.[/yellow]")
            input("\nPress Enter to continue...")
            return

        table = Table(title="Counterparties")
        table.add_column("ID", style="cyan")
        table.add_column("Name", style="green")
        table.add_column("LEI", style="yellow")
        for cp in counterparties:
            table.add_row(str(cp['id']), cp['name'], cp.get('lei', 'N/A'))
        self.console.print(table)

        choice = Prompt.ask("\nEnter a Counterparty ID to view their swaps (or press Enter to go back)")
        if choice.isdigit():
            self._view_swaps_for_counterparty(int(choice))

    def _view_swaps_for_counterparty(self, counterparty_id: int):
        """Display all swaps for a given counterparty."""
        swaps = self.db.get_swaps_by_counterparty_id(counterparty_id)
        if not swaps:
            self.console.print(f"[yellow]No swaps found for counterparty ID {counterparty_id}.[/yellow]")
        else:
            table = Table(title=f"Swaps for Counterparty ID {counterparty_id}")
            table.add_column("Contract ID", style="cyan")
            table.add_column("Reference Entity", style="magenta")
            table.add_column("Notional", style="green")
            table.add_column("Maturity", style="blue")

            for swap in swaps:
                table.add_row(
                    swap['contract_id'],
                    swap['reference_entity'],
                    f"{swap['currency']} {swap['notional_amount']:,.2f}",
                    swap['maturity_date']
                )
            self.console.print(table)

            choice = Prompt.ask("\nEnter a Contract ID to explain the swap (or press Enter to go back)")
            if choice:
                self._explain_swap(choice)
        
        input("\nPress Enter to continue...")

    def _list_all_reference_securities(self):
        """List all reference securities and view their swaps."""
        self.console.clear()
        print_ascii_art()
        self.console.print("\n[bold blue]All Reference Securities[/bold blue]\n")
        
        securities = self.db.get_all_reference_securities()
        if not securities:
            self.console.print("[yellow]No reference securities found in the database.[/yellow]")
            input("\nPress Enter to continue...")
            return

        table = Table(title="Reference Securities")
        table.add_column("ID", style="cyan")
        table.add_column("Identifier", style="green")
        table.add_column("Type", style="yellow")
        table.add_column("Description", style="blue")
        for sec in securities:
            table.add_row(str(sec['id']), sec['identifier'], sec.get('security_type', 'N/A'), sec.get('description', ''))
        self.console.print(table)

        choice = Prompt.ask("\nEnter a Security ID to view its swaps (or press Enter to go back)")
        if choice.isdigit():
            self._view_swaps_for_security(int(choice))

    def _view_swaps_for_security(self, security_id: int):
        """Display all swaps for a given reference security."""
        swaps = self.db.get_swaps_by_security_id(security_id)
        if not swaps:
            self.console.print(f"[yellow]No swaps found for security ID {security_id}.[/yellow]")
        else:
            table = Table(title=f"Swaps for Security ID {security_id}")
            table.add_column("Contract ID", style="cyan")
            table.add_column("Counterparty", style="magenta")
            table.add_column("Notional", style="green")
            table.add_column("Maturity", style="blue")

            for swap in swaps:
                table.add_row(
                    swap['contract_id'],
                    swap['counterparty'],
                    f"{swap['currency']} {swap['notional_amount']:,.2f}",
                    swap['maturity_date']
                )
            self.console.print(table)

            choice = Prompt.ask("\nEnter a Contract ID to explain the swap (or press Enter to go back)")
            if choice:
                self._explain_swap(choice)
        
        input("\nPress Enter to continue...")

    def _run_analysis_for_entity(self, original_question: str, entity: Dict):
        """Helper to run analysis once an entity is confirmed."""
        with self.console.status("[bold green]AI is analyzing...[/]"):
            context_data = self.ai_analyst._retrieve_context_data(entity)
            prompt = self.ai_analyst._generate_rag_prompt(original_question, context_data)
            response = self.ai_analyst.generate_final_analysis(prompt)
        
        if response['type'] == 'analysis':
            self.console.print("\n[bold]AI Analyst Response:[/bold]")
            self.console.print(response['message'])
        else:
            self.console.print(f"[red]Error: {response['message']}[/red]")

    def _download_data_for_entity(self, entity_name: str):
        """Helper to find and download data for a new entity."""
        with self.console.status(f"[bold green]Searching for '{entity_name}' on SEC EDGAR...[/]"):
            company_info = self.sec_handler.get_company_info(entity_name)
        
        if not company_info:
            self.console.print(f"[red]Could not find '{entity_name}' on SEC EDGAR.[/red]")
            return

        self.console.print(f"[green]Found company: {company_info.name} (CIK: {company_info.primary_identifiers.cik})[/green]")
        if Prompt.ask("\nDownload its latest filings?", choices=["y", "n"]) == 'y':
            self._download_filings_for_company(company_info)

    def _explain_swap(self, contract_id: str):
        """Get and display an AI-generated explanation for a swap."""
        self.console.clear()
        print_ascii_art()
        self.console.print(f"\n[bold blue]Swap Explanation for {contract_id}[/bold blue]\n")

        with self.console.status("[bold green]Generating explanation with Ollama...[/]"):
            explanation = self.swaps_analyzer.explain_swap(contract_id)
        
        if explanation:
            self.console.print(explanation)
        else:
            self.console.print("[red]Could not generate an explanation for this swap.[/red]")


    def _file_browser(self, start_path: Path) -> Optional[Path]:
        """An interactive file browser to select a file."""
        current_path = start_path.resolve()

        while True:
            self.console.clear()
            print_ascii_art()
            self.console.print(f"\n[bold blue]File Browser[/bold blue]")
            self.console.print(f"Current Path: [yellow]{current_path}[/yellow]")

            try:
                items = sorted(list(current_path.iterdir()), key=lambda p: (p.is_file(), p.name.lower()))
            except FileNotFoundError:
                self.console.print(f"[red]Error: Directory not found: {current_path}[/red]")
                current_path = start_path.resolve() # Reset to start path
                input("\nPress Enter to continue...")
                continue

            table = Table(title="Select a file or directory")
            table.add_column("#", style="cyan")
            table.add_column("Name", style="green")
            table.add_column("Type", style="yellow")

            # Add navigation options
            table.add_row("0", ".. (Parent Directory)", "[nav]")
            table.add_row("q", "Quit Browser", "[nav]")

            # Add files and directories
            for i, item in enumerate(items, 1):
                item_type = "File" if item.is_file() else "Directory"
                table.add_row(str(i), item.name, item_type)

            self.console.print(table)
            choice = Prompt.ask("\nEnter number to select, or 'q' to quit")

            if choice.lower() == 'q':
                return None
            elif choice == '0':
                current_path = current_path.parent
            elif choice.isdigit() and 1 <= int(choice) <= len(items):
                selected_item = items[int(choice) - 1]
                if selected_item.is_dir():
                    current_path = selected_item
                else:
                    return selected_item # Return the selected file path
            else:
                self.console.print("[red]Invalid selection.[/red]")
                input("\nPress Enter to continue...")

    def _ai_analyst_menu(self):
        """Menu for interacting with the AI Analyst."""
        self.console.clear()
        print_ascii_art()
        self.console.print("\n[bold blue]AI Analyst[/bold blue]\n")
        self.console.print("Ask a question about a company or security, e.g., 'Summarize the risk for Goldman Sachs'.")

        question = Prompt.ask("\nYour question (or press Enter to go back)")
        if not question:
            return

        with self.console.status("[bold green]AI is thinking...[/]"):
            response = self.ai_analyst.answer(question)

        if response['type'] == 'error':
            self.console.print(f"[red]Error: {response['message']}[/red]")

        elif response['type'] == 'prompt_download':
            if Prompt.ask(f"\n{response['message']} (y/n)") == 'y':
                self._download_data_for_entity(response['entity_name'])

        elif response['type'] == 'prompt_confirm_entity':
            if Prompt.ask(f"\n{response['message']} (y/n)") == 'y':
                self._run_analysis_for_entity(question, response['suggestion'])

        elif response['type'] == 'analysis':
            self.console.print("\n[bold]AI Analyst Response:[/bold]")
            self.console.print(response['message'])

        input("\nPress Enter to continue...")

    def _run_analysis_for_entity(self, original_question: str, entity: Dict):
        """Helper to run analysis once an entity is confirmed."""
        with self.console.status("[bold green]AI is analyzing...[/]"):
            context_data = self.ai_analyst._retrieve_context_data(entity)
            prompt = self.ai_analyst._generate_rag_prompt(original_question, context_data)
            answer = self.ai_analyst.ollama.generate(prompt, max_tokens=1024)
        
        self.console.print("\n[bold]AI Analyst Response:[/bold]")
        self.console.print(answer or "Failed to generate a response.")

    def _download_data_for_entity(self, entity_name: str):
        """Helper to find and download data for a new entity."""
        with self.console.status(f"[bold green]Searching for '{entity_name}' on SEC EDGAR...[/]"):
            company_info = self.sec.get_company_info(entity_name)
        
        if not company_info:
            self.console.print(f"[red]Could not find '{entity_name}' on SEC EDGAR.[/red]")
            return

        self.console.print(f"[green]Found company: {company_info.name} (CIK: {company_info.primary_identifiers.cik})[/green]")
        if Prompt.ask("\nDownload its latest filings?", choices=["y", "n"]) == 'y':
            self._download_filings_for_company(company_info)

def main():
    menu = MenuSystem()
    menu.main_menu()

if __name__ == "__main__":
    main()
