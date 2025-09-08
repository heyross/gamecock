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
