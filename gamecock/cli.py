"""Command line interface for SEC filing analysis."""
import click
from loguru import logger
import sys
from typing import Optional

from gamecock import __version__
from gamecock.forms import SECForm
from gamecock.menu_system import MenuSystem

@click.group()
@click.version_option(version=__version__)
def cli():
    """SEC filing analysis tool."""
    pass

@cli.command()
@click.option('--debug/--no-debug', default=False, help='Enable debug logging')
def menu(debug: bool):
    """Launch interactive menu system."""
    if debug:
        logger.remove()  # Remove default handler
        logger.add(sys.stderr, level="DEBUG")
    
    menu_system = MenuSystem()
    menu_system.main_menu()

if __name__ == '__main__':
    cli()
