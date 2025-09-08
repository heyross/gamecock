"""Command line interface for SEC filing analysis."""
import click
from loguru import logger
import sys
from typing import Optional
from rich.status import Status
from rich.console import Console

from gamecock import __version__
from gamecock.forms import SECForm
from gamecock.menu_system import MenuSystem
from gamecock.downloader import SECDownloader
from gamecock.swaps_analyzer import SwapsAnalyzer
from gamecock.ai_analyst import AIAnalyst
from gamecock.setup_handler import SetupHandler
from datetime import datetime, timedelta

@click.group()
@click.version_option(version=__version__)
def cli():
    """SEC filing analysis tool."""
    try:
        # Run setup checks before any command
        setup = SetupHandler()
        setup.run_all_checks()
    except Exception as e:
        print(f"An unexpected error occurred during startup: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

@cli.command()
@click.option('--debug/--no-debug', default=False, help='Enable debug logging')
def menu(debug: bool):
    """Launch interactive menu system."""
    if debug:
        logger.remove()  # Remove default handler
        logger.add(sys.stderr, level="DEBUG")
    
    menu_system = MenuSystem()
    menu_system.main_menu()

@cli.command()
@click.option('--cik', required=True, help='The CIK of the company to download filings for.')
@click.option('--years', default=1, help='The number of years of filings to download.')
def download(cik: str, years: int):
    """Download filings for a specific company."""
    logger.info(f"Initiating download for CIK: {cik} for the last {years} year(s).")
    downloader = SECDownloader()
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365 * years)
    
    with Status("[bold green]Downloading filings...[/]") as status:
        downloaded_files = downloader.download_company_filings(
            cik=cik,
            start_date=start_date,
            end_date=end_date
        )
    
    if downloaded_files:
        logger.info(f"Successfully downloaded {len(downloaded_files)} filings.")
    else:
        logger.warning("No filings were downloaded.")

@cli.command()
@click.option('--entity', required=True, help='The name of the reference entity (e.g., a security ticker or counterparty name) to analyze.')
def analyze(entity: str):
    """Run a risk analysis for a specific entity."""
    logger.info(f"Running risk analysis for: {entity}")
    analyzer = SwapsAnalyzer()
    report = analyzer.generate_risk_report(entity, include_analysis=True)

    if report.get("error"):
        logger.error(report["error"])
        return

    # Pretty print the report
    console = Console()
    console.print(f"\n[bold blue]Risk Report for {entity}[/bold blue]")
    console.print(f"[bold]Risk Score:[/bold] {report['risk_score']:.2f}/100 ({report['risk_level']})")
    console.print(f"[bold]Total Notional Exposure:[/bold] ${report['total_notional']:,.2f}")
    console.print(f"[bold]Number of Swaps:[/bold] {report['num_swaps']}")
    if 'ai_summary' in report:
        console.print("\n[bold]AI Summary:[/bold]")
        console.print(f"[italic]{report['ai_summary']}[/italic]")

@cli.command()
@click.option('--contract', required=True, help='The contract ID of the swap to explain.')
def explain(contract: str):
    """Generate a plain-language explanation of a swap contract."""
    logger.info(f"Generating explanation for swap: {contract}")
    analyzer = SwapsAnalyzer()
    
    with Status("[bold green]Generating explanation...[/]") as status:
        explanation = analyzer.explain_swap(contract)
    
    console = Console()
    console.print("\n[bold blue]AI-Generated Swap Explanation[/bold blue]")
    console.print(explanation)

if __name__ == '__main__':
    cli()
