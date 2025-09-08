"""Database handler for SEC and swaps data."""
import sqlite3
from pathlib import Path
from typing import List, Optional, Any, Dict
from loguru import logger

from .data_structures import CompanyInfo, EntityIdentifiers
from .db_swaps import SwapsDatabase, Swap, SwapObligation, SwapAnalysis, UnderlyingInstrument, ObligationTrigger

class DatabaseHandler:
    """Handles database operations for SEC data."""
    
    def __init__(self, db_path: Optional[str] = None, swaps_db_url: Optional[str] = None):
        """Initialize database connections.
        
        Args:
            db_path: Path to the SQLite database file for SEC data
            swaps_db_url: Database URL for swaps data (defaults to SQLite in data directory)
        """
        # Initialize SEC database
        if db_path is None:
            db_path = str(Path(__file__).parent.parent / "data" / "sec_data.db")
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        self._init_db()
        
        # Initialize swaps database
        if swaps_db_url is None:
            swaps_db_path = str(Path(__file__).parent.parent / "data" / "swaps.db")
            Path(swaps_db_path).parent.mkdir(parents=True, exist_ok=True)
            swaps_db_url = f"sqlite:///{swaps_db_path}"
            
        self.swaps_db = SwapsDatabase(swaps_db_url)
        
    def __del__(self):
        """Close database connections on deletion."""
        if hasattr(self, 'conn'):
            self.conn.close()
        if hasattr(self, 'swaps_db') and hasattr(self.swaps_db, 'engine'):
            self.swaps_db.engine.dispose()
        
    def _init_db(self):
        """Initialize database tables."""        
        # Companies table
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS companies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                cik TEXT UNIQUE,
                description TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Alternative tickers table
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS alt_tickers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_cik TEXT,
                symbol TEXT,
                exchange TEXT,
                security_type TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (company_cik) REFERENCES companies (cik)
            )
        """)
        
        # Filing details table
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS filings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_cik TEXT NOT NULL,
                accession_number TEXT NOT NULL,
                form_type TEXT NOT NULL,
                filing_date DATE NOT NULL,
                file_path TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (company_cik) REFERENCES companies (cik),
                UNIQUE(company_cik, accession_number)
            )
        """)
        
        # Filing IDs table
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS filing_ids (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_cik TEXT,
                filing_id TEXT,
                FOREIGN KEY (company_cik) REFERENCES companies (cik)
            )
        """)
        
        # Related entities table
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS related_entities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_cik TEXT,
                name TEXT NOT NULL,
                cik TEXT,
                description TEXT,
                relationship TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (company_cik) REFERENCES companies (cik)
            )
        """)
        
        self.conn.commit()
        
    # Swaps database methods
    def save_swap(self, swap_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Save a swap contract to the database.
        
        Args:
            swap_data: Dictionary containing swap data
            
        Returns:
            Dictionary containing the saved swap data or None if failed
        """
        return self.swaps_db.save_swap(swap_data)
    
    def get_swap(self, contract_id: str) -> Optional[Dict[str, Any]]:
        """Get a swap by contract ID.
        
        Args:
            contract_id: Unique identifier for the swap contract
            
        Returns:
            Dictionary containing swap data or None if not found
        """
        return self.swaps_db.get_swap(contract_id)
    
    def find_swaps_by_reference_entity(self, entity_name: str) -> List[Dict[str, Any]]:
        """Find all swaps for a reference entity.
        
        Args:
            entity_name: Name of the reference entity
            
        Returns:
            List of dictionaries containing swap data
        """
        return self.swaps_db.find_swaps_by_reference_entity(entity_name)
    
    def add_obligation(self, swap_id: int, obligation_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Add an obligation to a swap.
        
        Args:
            swap_id: ID of the swap
            obligation_data: Dictionary containing obligation data
            
        Returns:
            Dictionary containing the saved obligation data or None if failed
        """
        return self.swaps_db.add_obligation(swap_id, obligation_data)
    
    def save_analysis(self, swap_id: int, analysis_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Save analysis for a swap.
        
        Args:
            swap_id: ID of the swap
            analysis_data: Dictionary containing analysis data
            
        Returns:
            Dictionary containing the saved analysis data or None if failed
        """
        return self.swaps_db.save_analysis(swap_id, analysis_data)
    
    def get_swap_with_analysis(self, contract_id: str) -> Optional[Dict[str, Any]]:
        """Get a swap with its analysis and obligations.
        
        Args:
            contract_id: Unique identifier for the swap contract
            
        Returns:
            Dictionary containing swap data with analysis and obligations, or None if not found
        """
        return self.swaps_db.get_swap_with_analysis(contract_id)
    
    def delete_swap(self, contract_id: str) -> bool:
        """Delete a swap and all its related data.
        
        Args:
            contract_id: Unique identifier for the swap contract
            
        Returns:
            True if successful, False otherwise
        """
        return self.swaps_db.delete_swap(contract_id)
    
    def add_underlying_instrument(self, swap_id: int, instrument_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Add an underlying instrument to a swap.
        
        Args:
            swap_id: ID of the swap
            instrument_data: Dictionary containing instrument data
            
        Returns:
            Dictionary containing the saved instrument data or None if failed
        """
        return self.swaps_db.add_underlying_instrument(swap_id, instrument_data)
    
    def add_obligation_trigger(self, obligation_id: int, trigger_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Add a trigger to an obligation.
        
        Args:
            obligation_id: ID of the obligation
            trigger_data: Dictionary containing trigger data
            
        Returns:
            Dictionary containing the saved trigger data or None if failed
        """
        return self.swaps_db.add_obligation_trigger(obligation_id, trigger_data)
    
    def get_swap_obligations_view(self, swap_id: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get swap obligations view data.
        
        Args:
            swap_id: Optional swap ID to filter by
            
        Returns:
            List of dictionaries containing the swap obligations view data
        """
        return self.swaps_db.get_swap_obligations_view(swap_id)
    
    def get_obligations_by_counterparty(self, counterparty: str) -> List[Dict[str, Any]]:
        """Get all obligations for a specific counterparty.
        
        Args:
            counterparty: Name of the counterparty
            
        Returns:
            List of dictionaries containing obligation data
        """
        return self.swaps_db.get_obligations_by_counterparty(counterparty)
    
    def get_obligations_by_instrument(self, instrument_identifier: str) -> List[Dict[str, Any]]:
        """Get all obligations related to a specific instrument.
        
        Args:
            instrument_identifier: Identifier of the instrument (ticker, ISIN, etc.)
            
        Returns:
            List of dictionaries containing obligation data
        """
        return self.swaps_db.get_obligations_by_instrument(instrument_identifier)

    def get_all_counterparties(self) -> List[Dict[str, Any]]:
        """Get all counterparties from the database."""
        return self.swaps_db.get_all_counterparties()

    def get_all_reference_securities(self) -> List[Dict[str, Any]]:
        """Get all reference securities from the database."""
        return self.swaps_db.get_all_reference_securities()

    def get_swaps_by_counterparty_id(self, counterparty_id: int) -> List[Dict[str, Any]]:
        """Get all swaps for a specific counterparty by their ID."""
        return self.swaps_db.get_swaps_by_counterparty_id(counterparty_id)

    def get_swaps_by_security_id(self, security_id: int) -> List[Dict[str, Any]]:
        """Get all swaps related to a specific reference security by its ID."""
        return self.swaps_db.get_swaps_by_security_id(security_id)
    
    # SEC Database methods
    def save_company(self, company: CompanyInfo) -> bool:
        """Save company information to database."""
        try:
            # Save primary company info
            primary = company.primary_identifiers
            
            # Try to update existing company first
            self.cursor.execute("""
                UPDATE companies 
                SET name = ?, description = ?, updated_at = datetime('now')
                WHERE cik = ?
            """, (primary.name, primary.description, primary.cik))
            
            # If no rows were updated, insert new company
            if self.cursor.rowcount == 0:
                self.cursor.execute("""
                    INSERT INTO companies (name, cik, description, created_at, updated_at)
                    VALUES (?, ?, ?, datetime('now'), datetime('now'))
                """, (primary.name, primary.cik, primary.description))
            
            # Delete existing tickers for this company
            self.cursor.execute("DELETE FROM alt_tickers WHERE company_cik = ?", (primary.cik,))
            
            # Save tickers
            if hasattr(primary, 'tickers') and primary.tickers:
                for ticker in primary.tickers:
                    self.cursor.execute("""
                        INSERT INTO alt_tickers (company_cik, symbol, exchange, security_type, created_at, updated_at)
                        VALUES (?, ?, ?, ?, datetime('now'), datetime('now'))
                    """, (primary.cik, ticker['symbol'], ticker.get('exchange'), ticker.get('security_type')))
            
            # Delete existing related entities for this company
            self.cursor.execute("DELETE FROM related_entities WHERE company_cik = ?", (primary.cik,))
            
            # Save related entities
            for entity in company.related_entities:
                self.cursor.execute("""
                    INSERT INTO related_entities (company_cik, name, cik, description, relationship, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, datetime('now'), datetime('now'))
                """, (primary.cik, entity.name, entity.cik, entity.description, entity.relationship))
                
                # Save related entity tickers if any
                if hasattr(entity, 'tickers') and entity.tickers:
                    for ticker in entity.tickers:
                        self.cursor.execute("""
                            INSERT INTO alt_tickers (company_cik, symbol, exchange, security_type, created_at, updated_at)
                            VALUES (?, ?, ?, ?, datetime('now'), datetime('now'))
                        """, (entity.cik, ticker['symbol'], ticker.get('exchange'), ticker.get('security_type')))
            
            self.conn.commit()
            return True
            
        except Exception as e:
            logger.error(f"Error saving company to database: {str(e)}")
            self.conn.rollback()
            return False
            
    def get_all_companies(self) -> List[CompanyInfo]:
        """Retrieve all saved companies."""
        try:
            companies = []
            self.cursor.execute("""
                SELECT c.name, c.cik, c.description, c.id
                FROM companies c
            """)
            
            for row in self.cursor.fetchall():
                company_id = row[3]
                
                # Get alternative tickers
                self.cursor.execute("SELECT symbol, exchange, security_type FROM alt_tickers WHERE company_cik = ?", (row[1],))
                alt_tickers = [{'symbol': r[0], 'exchange': r[1], 'security_type': r[2]} for r in self.cursor.fetchall()]
                
                # Get filing IDs
                self.cursor.execute("SELECT filing_id FROM filing_ids WHERE company_cik = ?", (row[1],))
                filing_ids = [r[0] for r in self.cursor.fetchall()]
                
                companies.append(CompanyInfo(
                    name=row[0],
                    cik=row[1],
                    description=row[2],
                    alt_tickers=alt_tickers if alt_tickers else None,
                    filing_ids=filing_ids if filing_ids else None
                ))
                
            return companies
                
        except Exception as e:
            logger.error(f"Error retrieving companies: {str(e)}")
            return []
            
    def get_company_by_cik(self, cik: str) -> Optional[CompanyInfo]:
        """Retrieve a company by CIK."""
        try:
            self.cursor.execute("""
                SELECT c.name, c.cik, c.description, c.id
                FROM companies c
                WHERE c.cik = ?
            """, (cik,))
            
            row = self.cursor.fetchone()
            if row:
                company_id = row[3]
                
                # Get alternative tickers
                self.cursor.execute("SELECT symbol, exchange, security_type FROM alt_tickers WHERE company_cik = ?", (row[1],))
                alt_tickers = [{'symbol': r[0], 'exchange': r[1], 'security_type': r[2]} for r in self.cursor.fetchall()]
                
                # Get filing IDs
                self.cursor.execute("SELECT filing_id FROM filing_ids WHERE company_cik = ?", (row[1],))
                filing_ids = [r[0] for r in self.cursor.fetchall()]
                
                return CompanyInfo(
                    name=row[0],
                    cik=row[1],
                    description=row[2],
                    alt_tickers=alt_tickers if alt_tickers else None,
                    filing_ids=filing_ids if filing_ids else None
                )
            return None
                
        except Exception as e:
            logger.error(f"Error retrieving company: {str(e)}")
            return None
