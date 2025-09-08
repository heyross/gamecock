"""Handles first-run setup, prerequisite checks, and Ollama validation."""
import subprocess
import sys
from pathlib import Path
from loguru import logger
from rich.console import Console
from rich.status import Status

from .ollama_handler import OllamaHandler

console = Console()

class SetupHandler:
    """Manages the initial setup and validation for the application."""

    def __init__(self, data_dir: Path = Path("data")):
        self.data_dir = data_dir
        self.setup_complete_flag = self.data_dir / ".setup_complete"
        self.ollama = OllamaHandler()

    def run_all_checks(self):
        """Run all necessary setup and validation checks."""
        logger.info("Running initial setup checks...")
        self.check_and_install_prerequisites()
        self.validate_ollama_setup()
        logger.info("All setup checks passed.")

    def check_and_install_prerequisites(self):
        """Check if prerequisites are installed, and if not, install them."""
        if self.setup_complete_flag.exists():
            logger.info("Prerequisite check already completed. Skipping.")
            return

        console.print("[bold yellow]First-time setup: Installing required packages...[/bold yellow]")
        try:
            with Status("[bold green]Running pip install -r requirements.txt...[/]") as status:
                # Using python -m pip to ensure we use the pip from the correct environment
                result = subprocess.run(
                    [sys.executable, "-m", "pip", "install", "-r", "requirements.txt"],
                    capture_output=True,
                    text=True,
                    check=True
                )
                logger.debug(result.stdout)
            console.print("[green]Prerequisites installed successfully.[/green]")
            # Create the flag file to indicate setup is complete
            self.data_dir.mkdir(exist_ok=True)
            self.setup_complete_flag.touch()
        except subprocess.CalledProcessError as e:
            console.print("[bold red]Error installing prerequisites:[/bold red]")
            console.print(e.stderr)
            console.print("Please try running 'pip install -r requirements.txt' manually.")
            sys.exit(1)
        except FileNotFoundError:
            console.print("[bold red]Error: 'requirements.txt' not found.[/bold red]")
            console.print("Please ensure you are running the application from the project's root directory.")
            sys.exit(1)

    def validate_ollama_setup(self):
        """Check for Ollama and ensure the required model is available."""
        logger.info("Validating Ollama setup...")
        with Status("[bold green]Checking Ollama service...[/]") as status:
            if not self.ollama.is_running():
                console.print("[bold red]Ollama service is not running.[/bold red]")
                console.print("Please start the Ollama application and then restart this tool.")
                sys.exit(1)
            status.update("[bold green]Ollama service is running. Checking for model...[/]")
            
            if not self.ollama.is_model_available():
                console.print(f"[yellow]Model '{self.ollama.model}' not found.[/yellow]")
                if console.input("Would you like to download it now? (y/n) ").lower() == 'y':
                    self.ollama.pull_model()
                else:
                    console.print("The required model is not available. The AI Analyst features will not work.")
                    console.print(f"You can download it manually by running: 'ollama pull {self.ollama.model}'")
            else:
                logger.info(f"Required model '{self.ollama.model}' is available.")
