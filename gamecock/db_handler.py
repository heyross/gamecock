"""Database handler for SEC data."""
import sqlite3
from pathlib import Path
from typing import List, Optional
from loguru import logger

from .data_structures import CompanyInfo, EntityIdentifiers

class DatabaseHandler:
    """Handles database operations for SEC data."""
    
    def __init__(self, db_path: Optional[str] = None):
        """Initialize database connection."""
        if db_path is None:
            db_path = str(Path(__file__).parent.parent / "data" / "sec_data.db")
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        self._init_db()
        
    def _init_db(self):
        """Initialize database tables."""
        # Drop existing tables if they exist
        self.cursor.execute("DROP TABLE IF EXISTS filing_ids")
        self.cursor.execute("DROP TABLE IF EXISTS alt_tickers")
        self.cursor.execute("DROP TABLE IF EXISTS related_entities")
        self.cursor.execute("DROP TABLE IF EXISTS companies")
        
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
        
    def save_company(self, company: CompanyInfo) -> bool:
        """Save company information to database."""
        try:
            # Save primary company info
            primary = company.primary_identifiers
            self.cursor.execute("""
                INSERT INTO companies (name, cik, description, created_at, updated_at)
                VALUES (?, ?, ?, datetime('now'), datetime('now'))
            """, (primary.name, primary.cik, primary.description))
            
            # Save tickers
            if hasattr(primary, 'tickers') and primary.tickers:
                for ticker in primary.tickers:
                    self.cursor.execute("""
                        INSERT INTO alt_tickers (company_cik, symbol, exchange, security_type, created_at, updated_at)
                        VALUES (?, ?, ?, ?, datetime('now'), datetime('now'))
                    """, (primary.cik, ticker['symbol'], ticker.get('exchange'), ticker.get('security_type')))
            
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
