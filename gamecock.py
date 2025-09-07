#!/usr/bin/env python
"""
Gamecock - SEC Filing Analysis Tool
Direct launch script for the Gamecock Menu System
"""
import os
from pathlib import Path
from dotenv import load_dotenv
from gamecock.menu_system import MenuSystem

if __name__ == '__main__':
    # Ensure we're in the correct directory for relative paths
    os.chdir(Path(__file__).parent)
    
    # Load environment variables
    load_dotenv()
    
    # Initialize and start menu system
    menu_system = MenuSystem()
    menu_system.main_menu()
