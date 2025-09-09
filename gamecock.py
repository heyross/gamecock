#!/usr/bin/env python
"""
Gamecock - SEC Filing Analysis Tool
Direct launch script for the Gamecock Menu System
"""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from loguru import logger
from gamecock.setup_handler import SetupHandler
from gamecock.menu_system import MenuSystem

def main():
    try:
        # Configure centralized logging (console + rotating file) FIRST
        try:
            logger.remove()
            logger.add(sys.stderr, level="DEBUG")
        except Exception:
            # logger may not be initialized yet, ignore
            pass

        # Ensure logs directory exists and attach file sink (relative to project root)
        logs_dir = Path(__file__).parent / "data" / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)
        app_log = logs_dir / "app.log"
        logger.add(
            str(app_log),
            level="DEBUG",
            rotation="10 MB",
            retention=10,
            compression="zip",
            enqueue=True,
            backtrace=True,
            diagnose=True,
            format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}",
        )
        logger.info("Logging initialized -> {}", app_log)

        # Run setup checks before starting the menu
        setup = SetupHandler()
        setup.run_all_checks()
        
        # Ensure we're in the correct directory for relative paths
        os.chdir(Path(__file__).parent)
        
        # Load environment variables
        load_dotenv()
        
        # Initialize and start menu system
        menu_system = MenuSystem()
        menu_system.main_menu()
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        import traceback
        traceback.print_exc()
        input("\nPress Enter to exit.")

if __name__ == "__main__":
    main()
