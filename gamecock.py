#!/usr/bin/env python
"""
Gamecock - SEC Filing Analysis Tool
Direct launch script for the Gamecock Menu System
"""
import os
from pathlib import Path
from dotenv import load_dotenv
from gamecock.setup_handler import SetupHandler
from gamecock.menu_system import MenuSystem

def main():
    try:
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
