"""Database handler for SEC and swaps data."""
from pathlib import Path
from typing import List, Optional, Any, Dict
from loguru import logger
from datetime import datetime

from .data_structures import CompanyInfo, EntityIdentifiers
from sqlalchemy import create_engine, Column, Integer, String, Float, Date, DateTime, Text, ForeignKey, JSON, Boolean, func, text, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, Session
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.exc import SQLAlchemyError

Base = declarative_base()

class Swap(Base):
    """Swap contract model."""
    __tablename__ = 'swaps'
    
    id = Column(Integer, primary_key=True)
    contract_id = Column(String(100), unique=True, nullable=False)
    counterparty_id = Column(Integer, ForeignKey('counterparties.id'), nullable=False)
    reference_entity = Column(String(255), nullable=False)
    notional_amount = Column(Float, nullable=False)
    currency = Column(String(10), nullable=False, default='USD')
    effective_date = Column(Date, nullable=False)
    maturity_date = Column(Date, nullable=False)
    swap_type = Column(String(50), nullable=True)
    payment_frequency = Column(String(50), nullable=True)
    fixed_rate = Column(Float, nullable=True)
    floating_rate_index = Column(String(100), nullable=True)
    floating_rate_spread = Column(Float, nullable=True)
    collateral_terms = Column(JSON, nullable=True)
    additional_terms = Column(JSON, nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    obligations = relationship("SwapObligation", back_populates="swap", cascade="all, delete-orphan")
    analysis = relationship("SwapAnalysis", back_populates="swap", uselist=False, cascade="all, delete-orphan")
    counterparty_rel = relationship("Counterparty", back_populates="swaps")
    underlying_instruments = relationship("UnderlyingInstrument", back_populates="swap", cascade="all, delete-orphan")
    
    def to_dict(self):
        return {
            'id': self.id,
            'contract_id': self.contract_id,
            'counterparty': self.counterparty_rel.name if self.counterparty_rel else None,
            'reference_entity': self.reference_entity,
            'notional_amount': self.notional_amount,
            'currency': self.currency,
            'effective_date': self.effective_date.isoformat() if self.effective_date else None,
            'maturity_date': self.maturity_date.isoformat() if self.maturity_date else None,
            'payment_frequency': self.payment_frequency,
            'fixed_rate': self.fixed_rate,
            'floating_rate_index': self.floating_rate_index,
            'floating_rate_spread': self.floating_rate_spread,
            'collateral_terms': self.collateral_terms,
            'additional_terms': self.additional_terms,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }

class SwapObligation(Base):
    """Swap obligation model."""
    __tablename__ = 'swap_obligations'
    
    id = Column(Integer, primary_key=True)
    swap_id = Column(Integer, ForeignKey('swaps.id', ondelete='CASCADE'), nullable=False)
    obligation_type = Column(String(50), nullable=False)
    amount = Column(Float, nullable=False)
    currency = Column(String(10), nullable=False, default='USD')
    due_date = Column(Date, nullable=True)
    status = Column(String(50), nullable=True, default='pending')
    description = Column(Text, nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    swap = relationship("Swap", back_populates="obligations")
    triggers = relationship("ObligationTrigger", back_populates="obligation", cascade="all, delete-orphan")
    
    def to_dict(self):
        return {
            'id': self.id,
            'swap_id': self.swap_id,
            'obligation_type': self.obligation_type,
            'amount': self.amount,
            'currency': self.currency,
            'due_date': self.due_date.isoformat() if self.due_date else None,
            'status': self.status,
            'description': self.description,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }

class SwapAnalysis(Base):
    """Swap analysis model."""
    __tablename__ = 'swap_analysis'
    
    id = Column(Integer, primary_key=True)
    swap_id = Column(Integer, ForeignKey('swaps.id', ondelete='CASCADE'), nullable=False, unique=True)
    analysis_text = Column(Text, nullable=True)
    risk_score = Column(Float, nullable=True)
    key_risks = Column(JSON, nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    swap = relationship("Swap", back_populates="analysis")
    
    def to_dict(self):
        return {
            'id': self.id,
            'swap_id': self.swap_id,
            'analysis_text': self.analysis_text,
            'risk_score': self.risk_score,
            'key_risks': self.key_risks,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }

class ReferenceSecurity(Base):
    """Represents a reference security in a swap contract."""
    __tablename__ = 'reference_securities'
    
    id = Column(Integer, primary_key=True)
    identifier = Column(String(100), unique=True, nullable=False)
    security_type = Column(String(50), nullable=True)
    description = Column(Text, nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    underlying_instruments = relationship("UnderlyingInstrument", back_populates="security_rel")

    def to_dict(self):
        return {
            'id': self.id,
            'identifier': self.identifier,
            'security_type': self.security_type,
            'description': self.description,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }

class UnderlyingInstrument(Base):
    """Represents an underlying instrument in a swap contract."""
    __tablename__ = 'underlying_instruments'
    
    id = Column(Integer, primary_key=True)
    swap_id = Column(Integer, ForeignKey('swaps.id', ondelete='CASCADE'), nullable=False)
    instrument_type = Column(String(50), nullable=False)
    security_id = Column(Integer, ForeignKey('reference_securities.id'), nullable=False)
    description = Column(Text, nullable=True)
    quantity = Column(Float, nullable=True)
    notional_amount = Column(Float, nullable=True)
    currency = Column(String(10), nullable=True, default='USD')
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    swap = relationship("Swap", back_populates="underlying_instruments")
    security_rel = relationship("ReferenceSecurity", back_populates="underlying_instruments")
    
    def to_dict(self):
        return {
            'id': self.id,
            'swap_id': self.swap_id,
            'instrument_type': self.instrument_type,
            'identifier': self.security_rel.identifier if self.security_rel else None,
            'description': self.description,
            'quantity': self.quantity,
            'notional_amount': self.notional_amount,
            'currency': self.currency,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }

class Counterparty(Base):
    """Represents a counterparty in a swap contract."""
    __tablename__ = 'counterparties'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(255), unique=True, nullable=False)
    lei = Column(String(20), unique=True, nullable=True)
    entity_type = Column(String(50), nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    swaps = relationship("Swap", back_populates="counterparty_rel")

    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'lei': self.lei,
            'entity_type': self.entity_type,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }

class ObligationTrigger(Base):
    """Represents a trigger condition for a swap obligation."""
    __tablename__ = 'obligation_triggers'
    
    id = Column(Integer, primary_key=True)
    obligation_id = Column(Integer, ForeignKey('swap_obligations.id', ondelete='CASCADE'), nullable=False)
    trigger_type = Column(String(50), nullable=False)
    trigger_condition = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    is_active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    obligation = relationship("SwapObligation", back_populates="triggers")
    
    def to_dict(self):
        return {
            'id': self.id,
            'obligation_id': self.obligation_id,
            'trigger_type': self.trigger_type,
            'trigger_condition': self.trigger_condition,
            'description': self.description,
            'is_active': self.is_active,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }


class Filing(Base):
    """Metadata for downloaded filings for stats and navigation."""
    __tablename__ = 'filings'
    __table_args__ = (
        UniqueConstraint('company_cik', 'accession_number', name='uix_company_accession'),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    company_cik = Column(String, nullable=False)
    accession_number = Column(String, nullable=False)
    form_type = Column(String, nullable=True)
    filing_date = Column(String, nullable=True)
    file_path = Column(Text, nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

class Company(Base):
    __tablename__ = 'companies'
    cik = Column(String, primary_key=True)
    name = Column(String)
    description = Column(Text)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    alt_tickers = relationship("AltTicker", back_populates="company", cascade="all, delete-orphan")
    related_entities = relationship("RelatedEntity", back_populates="company", cascade="all, delete-orphan")

class AltTicker(Base):
    __tablename__ = 'alt_tickers'
    id = Column(Integer, primary_key=True)
    company_cik = Column(String, ForeignKey('companies.cik'))
    symbol = Column(String)
    exchange = Column(String)
    security_type = Column(String)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    company = relationship("Company", back_populates="alt_tickers")

class RelatedEntity(Base):
    __tablename__ = 'related_entities'
    id = Column(Integer, primary_key=True)
    company_cik = Column(String, ForeignKey('companies.cik'))
    name = Column(String)
    cik = Column(String)
    description = Column(Text)
    relationship_type = Column(String)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    company = relationship("Company", back_populates="related_entities")

class DatabaseHandler:
    """Handles all database operations for the application."""

    def __init__(self, db_url: Optional[str] = None):
        """Initialize a unified database connection.

        Args:
            db_url: The database connection URL. Defaults to a file in the data directory.
        """
        if db_url is None:
            db_path = Path(__file__).parent.parent / "data" / "gamecock.db"
            db_path.parent.mkdir(parents=True, exist_ok=True)
            db_url = f"sqlite:///{db_path}"

        self.engine = create_engine(db_url, connect_args={"check_same_thread": False} if db_url.startswith("sqlite") else {})
        self.Session = sessionmaker(bind=self.engine)

        # Initialize all tables
        Base.metadata.create_all(self.engine)
        self._create_view()




    def __del__(self):
        """Close database connections on deletion."""
        if hasattr(self, 'engine'):
            self.engine.dispose()
        

    def _create_view(self):
        """Create the database view for swap obligations."""
        view_sql = """
        CREATE VIEW IF NOT EXISTS vw_swap_obligations AS
        SELECT 
            s.id AS swap_id,
            s.contract_id,
            c.name AS counterparty,
            s.reference_entity,
            s.notional_amount,
            s.currency,
            s.effective_date,
            s.maturity_date,
            s.swap_type,
            o.id AS obligation_id,
            o.obligation_type,
            o.amount AS obligation_amount,
            o.currency AS obligation_currency,
            o.due_date,
            o.status AS obligation_status,
            o.description AS obligation_description,
            ui.instrument_type,
            rs.identifier AS instrument_identifier,
            rs.description AS instrument_description,
            ui.quantity,
            ui.notional_amount AS instrument_notional,
            ot.trigger_type,
            ot.trigger_condition,
            ot.description AS trigger_description
        FROM 
            swaps s
        LEFT JOIN 
            counterparties c ON s.counterparty_id = c.id
        LEFT JOIN 
            swap_obligations o ON s.id = o.swap_id
        LEFT JOIN 
            underlying_instruments ui ON s.id = ui.swap_id
        LEFT JOIN
            reference_securities rs ON ui.security_id = rs.id
        LEFT JOIN 
            obligation_triggers ot ON o.id = ot.obligation_id
        WHERE 
            (ot.is_active = 1 OR ot.id IS NULL)
        """
        session = self.Session()
        try:
            session.execute(text(view_sql))
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Error creating view: {str(e)}")
        finally:
            session.close()

    def get_all_companies(self) -> List[CompanyInfo]:
        """Return all saved companies as CompanyInfo objects.

        This supports menu_system browsing and download flows which expect
        structured CompanyInfo with primary identifiers and related entities.
        """
        session = self.Session()
        companies: List[CompanyInfo] = []
        try:
            rows = session.query(Company).order_by(Company.name).all()
            for row in rows:
                # Build tickers list
                tickers = []
                for t in row.alt_tickers:
                    tickers.append({
                        'symbol': t.symbol,
                        'exchange': t.exchange,
                        'security_type': t.security_type,
                    })

                primary = EntityIdentifiers(
                    name=row.name,
                    cik=row.cik,
                    description=row.description,
                    tickers=tickers,
                )

                related_list = []
                for r in row.related_entities:
                    related_list.append(
                        EntityIdentifiers(
                            name=r.name,
                            cik=r.cik,
                            description=r.description,
                            relationship_type=r.relationship_type,
                        )
                    )

                companies.append(CompanyInfo(name=row.name, primary_identifiers=primary, related_entities=related_list))
        except SQLAlchemyError as e:
            logger.error(f"Error retrieving companies: {str(e)}")
        finally:
            session.close()
        return companies

    def get_or_create_counterparty(self, name: str) -> Counterparty:
        """Get an existing counterparty or create a new one."""
        session = self.Session()
        try:
            counterparty = session.query(Counterparty).filter(func.lower(Counterparty.name) == name.lower()).first()
            if not counterparty:
                counterparty = Counterparty(name=name)
                session.add(counterparty)
                session.commit()
                logger.info(f"Created new counterparty: {name}")
            return counterparty
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Error getting or creating counterparty '{name}': {e}")
            raise
        finally:
            session.close()

    def get_or_create_security(self, identifier: str) -> ReferenceSecurity:
        """Get an existing reference security or create a new one."""
        session = self.Session()
        try:
            security = session.query(ReferenceSecurity).filter(func.lower(ReferenceSecurity.identifier) == identifier.lower()).first()
            if not security:
                security = ReferenceSecurity(identifier=identifier)
                session.add(security)
                session.commit()
                logger.info(f"Created new reference security: {identifier}")
            return security
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Error getting or creating security '{identifier}': {e}")
            raise
        finally:
            session.close()

    def save_swap(self, swap_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Save a swap contract to the database.
        
        Args:
            swap_data: Dictionary containing swap data
            
        Returns:
            Dictionary containing the saved swap data or None if failed
        """
        session = self.Session()
        try:
            counterparty_name = swap_data.pop('counterparty', None)
            if not counterparty_name:
                raise ValueError("Counterparty name is required to save a swap.")

            counterparty = session.query(Counterparty).filter_by(name=counterparty_name).first()
            if not counterparty:
                counterparty = Counterparty(name=counterparty_name)
                session.add(counterparty)
                session.flush()

            existing_swap = session.query(Swap).filter_by(contract_id=swap_data['contract_id']).first()
            
            for date_field in ['effective_date', 'maturity_date']:
                if date_field in swap_data and isinstance(swap_data[date_field], str):
                    swap_data[date_field] = datetime.strptime(swap_data[date_field], '%Y-%m-%d').date()
            
            swap_data['counterparty_id'] = counterparty.id

            if existing_swap:
                for key, value in swap_data.items():
                    if hasattr(existing_swap, key) and key != 'id':
                        setattr(existing_swap, key, value)
                existing_swap.updated_at = datetime.utcnow()
                swap = existing_swap
            else:
                swap = Swap(**swap_data)
                session.add(swap)
            
            session.commit()
            return swap.to_dict()
            
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Error saving swap: {str(e)}")
            return None
        finally:
            session.close()
    
    def get_swap(self, contract_id: str) -> Optional[Dict[str, Any]]:
        """Get a swap by contract ID.
        
        Args:
            contract_id: Unique identifier for the swap contract
            
        Returns:
            Dictionary containing swap data or None if not found
        """
        session = self.Session()
        try:
            swap = session.query(Swap).filter_by(contract_id=contract_id).first()
            return swap.to_dict() if swap else None
        except SQLAlchemyError as e:
            logger.error(f"Error getting swap: {str(e)}")
            return None
        finally:
            session.close()
    
    def find_swaps_by_reference_entity(self, entity_name: str) -> List[Dict[str, Any]]:
        """Find all swaps for a reference entity.
        
        Args:
            entity_name: Name of the reference entity
            
        Returns:
            List of dictionaries containing swap data
        """
        session = self.Session()
        try:
            swaps = session.query(Swap).filter(
                Swap.reference_entity.ilike(f"%{entity_name}%")
            ).all()
            return [swap.to_dict() for swap in swaps]
        except SQLAlchemyError as e:
            logger.error(f"Error finding swaps by reference entity: {str(e)}")
            return []
        finally:
            session.close()
    
    def add_obligation(self, swap_id: int, obligation_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Add an obligation to a swap.
        
        Args:
            swap_id: ID of the swap
            obligation_data: Dictionary containing obligation data
            
        Returns:
            Dictionary containing the saved obligation data or None if failed
        """
        session = self.Session()
        try:
            obligation_data['swap_id'] = swap_id
            obligation = SwapObligation(**obligation_data)
            session.add(obligation)
            session.commit()
            return obligation.to_dict()
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Error adding obligation: {str(e)}")
            return None
        finally:
            session.close()
    
    def save_analysis(self, swap_id: int, analysis_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Save analysis for a swap.
        
        Args:
            swap_id: ID of the swap
            analysis_data: Dictionary containing analysis data
            
        Returns:
            Dictionary containing the saved analysis data or None if failed
        """
        session = self.Session()
        try:
            analysis = session.query(SwapAnalysis).filter_by(swap_id=swap_id).first()
            
            if analysis:
                for key, value in analysis_data.items():
                    if hasattr(analysis, key) and key != 'id':
                        setattr(analysis, key, value)
                analysis.updated_at = datetime.utcnow()
            else:
                analysis_data['swap_id'] = swap_id
                analysis = SwapAnalysis(**analysis_data)
                session.add(analysis)
            
            session.commit()
            return analysis.to_dict()
            
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Error saving analysis: {str(e)}")
            return None
        finally:
            session.close()
    
    def get_swap_with_analysis(self, contract_id: str) -> Optional[Dict[str, Any]]:
        """Get a swap with its analysis and obligations.
        
        Args:
            contract_id: Unique identifier for the swap contract
            
        Returns:
            Dictionary containing swap data with analysis and obligations, or None if not found
        """
        session = self.Session()
        try:
            swap = session.query(Swap).filter_by(contract_id=contract_id).first()
            if not swap:
                return None
                
            result = swap.to_dict()
            
            if swap.analysis:
                result['analysis'] = swap.analysis.to_dict()
            
            result['obligations'] = [obligation.to_dict() for obligation in swap.obligations]
            
            return result
            
        except SQLAlchemyError as e:
            logger.error(f"Error getting swap with analysis: {str(e)}")
            return None
        finally:
            session.close()
    
    def delete_swap(self, contract_id: str) -> bool:
        """Delete a swap and all its related data.
        
        Args:
            contract_id: Unique identifier for the swap contract
            
        Returns:
            True if successful, False otherwise
        """
        session = self.Session()
        try:
            swap = session.query(Swap).filter_by(contract_id=contract_id).first()
            if swap:
                session.delete(swap)
                session.commit()
                return True
            return False
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Error deleting swap: {str(e)}")
            return False
        finally:
            session.close()
    
    def add_underlying_instrument(self, swap_id: int, instrument_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Add an underlying instrument to a swap.
        
        Args:
            swap_id: ID of the swap
            instrument_data: Dictionary containing instrument data
            
        Returns:
            Dictionary containing the saved instrument data or None if failed
        """
        session = self.Session()
        try:
            security_identifier = instrument_data.pop('identifier', None)
            if not security_identifier:
                raise ValueError("Security identifier is required to add an instrument.")

            security = session.query(ReferenceSecurity).filter_by(identifier=security_identifier).first()
            if not security:
                security = ReferenceSecurity(
                    identifier=security_identifier,
                    security_type=instrument_data.get('instrument_type'),
                    description=instrument_data.get('description')
                )
                session.add(security)
                session.flush()

            instrument_data['security_id'] = security.id
            instrument = UnderlyingInstrument(swap_id=swap_id, **instrument_data)
            session.add(instrument)
            session.commit()
            return instrument.to_dict()
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Error adding underlying instrument: {str(e)}")
            return None
        finally:
            session.close()
    
    def add_obligation_trigger(self, obligation_id: int, trigger_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Add a trigger to an obligation.
        
        Args:
            obligation_id: ID of the obligation
            trigger_data: Dictionary containing trigger data
            
        Returns:
            Dictionary containing the saved trigger data or None if failed
        """
        session = self.Session()
        try:
            trigger = ObligationTrigger(obligation_id=obligation_id, **trigger_data)
            session.add(trigger)
            session.commit()
            return trigger.to_dict()
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Error adding obligation trigger: {str(e)}")
            return None
        finally:
            session.close()
    
    def get_swap_obligations_view(self, swap_id: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get swap obligations view data.
        
        Args:
            swap_id: Optional swap ID to filter by
            
        Returns:
            List of dictionaries containing the swap obligations view data
        """
        session = self.Session()
        try:
            query = "SELECT * FROM vw_swap_obligations"
            params = {}
            if swap_id is not None:
                query += " WHERE swap_id = :swap_id"
                params['swap_id'] = swap_id
            
            result = session.execute(text(query), params)
            columns = result.keys()
            return [dict(zip(columns, row)) for row in result.fetchall()]
        except SQLAlchemyError as e:
            logger.error(f"Error getting swap obligations view: {str(e)}")
            return []
        finally:
            session.close()
    
    def get_obligations_by_counterparty(self, counterparty: str) -> List[Dict[str, Any]]:
        """Get all obligations for a specific counterparty.
        
        Args:
            counterparty: Name of the counterparty
            
        Returns:
            List of dictionaries containing obligation data
        """
        session = self.Session()
        try:
            swaps = session.query(Swap).join(Counterparty).filter(Counterparty.name == counterparty).all()
            obligations = []
            for swap in swaps:
                for obligation in swap.obligations:
                    obligation_dict = obligation.to_dict()
                    obligation_dict['swap_contract_id'] = swap.contract_id
                    obligation_dict['reference_entity'] = swap.reference_entity
                    obligations.append(obligation_dict)
            return obligations
        except SQLAlchemyError as e:
            logger.error(f"Error getting obligations by counterparty: {str(e)}")
            return []
        finally:
            session.close()
    
    def get_obligations_by_instrument(self, instrument_identifier: str) -> List[Dict[str, Any]]:
        """Get all obligations related to a specific instrument.
        
        Args:
            instrument_identifier: Identifier of the instrument (ticker, ISIN, etc.)
            
        Returns:
            List of dictionaries containing obligation data
        """
        session = self.Session()
        try:
            instruments = session.query(UnderlyingInstrument).join(ReferenceSecurity).filter(ReferenceSecurity.identifier == instrument_identifier).all()
            obligations = []
            for instrument in instruments:
                swap = instrument.swap
                for obligation in swap.obligations:
                    obligation_dict = obligation.to_dict()
                    obligation_dict['swap_contract_id'] = swap.contract_id
                    obligation_dict['counterparty'] = swap.counterparty_rel.name
                    obligation_dict['instrument_type'] = instrument.instrument_type
                    obligation_dict['instrument_identifier'] = instrument.security_rel.identifier
                    obligations.append(obligation_dict)
            return obligations
        except SQLAlchemyError as e:
            logger.error(f"Error getting obligations by instrument: {str(e)}")
            return []
        finally:
            session.close()

    def get_all_counterparties(self) -> List[Dict[str, Any]]:
        """Get all counterparties from the database."""
        session = self.Session()
        try:
            counterparties = session.query(Counterparty).order_by(Counterparty.name).all()
            return [c.to_dict() for c in counterparties]
        except SQLAlchemyError as e:
            logger.error(f"Error getting all counterparties: {str(e)}")
            return []
        finally:
            session.close()

    def get_all_reference_securities(self) -> List[Dict[str, Any]]:
        """Get all reference securities from the database."""
        session = self.Session()
        try:
            securities = session.query(ReferenceSecurity).order_by(ReferenceSecurity.identifier).all()
            return [s.to_dict() for s in securities]
        except SQLAlchemyError as e:
            logger.error(f"Error getting all reference securities: {str(e)}")
            return []
        finally:
            session.close()

    # Filings helpers (ORM-based)
    def upsert_filing(self, company_cik: str, accession_number: str, form_type: Optional[str], filing_date: Optional[str], file_path: Optional[str]) -> None:
        """Insert or update a filing record using SQLAlchemy."""
        session = self.Session()
        try:
            filing = session.query(Filing).filter_by(company_cik=company_cik, accession_number=accession_number).first()
            if filing:
                filing.form_type = form_type
                filing.filing_date = filing_date
                filing.file_path = file_path
                filing.updated_at = datetime.utcnow()
            else:
                filing = Filing(
                    company_cik=company_cik,
                    accession_number=accession_number,
                    form_type=form_type,
                    filing_date=filing_date,
                    file_path=file_path,
                )
                session.add(filing)
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Error upserting filing {company_cik}/{accession_number}: {str(e)}")
        finally:
            session.close()

    def get_filings_stats(self) -> Dict[str, Any]:
        """Return basic statistics for filings for menu display."""
        session = self.Session()
        stats: Dict[str, Any] = {"total_filings": 0, "total_companies": 0, "latest_filing": None, "types": []}
        try:
            stats["total_filings"] = session.query(func.count(Filing.id)).scalar() or 0
            stats["total_companies"] = session.query(func.count(func.distinct(Filing.company_cik))).scalar() or 0
            stats["latest_filing"] = session.query(func.max(Filing.filing_date)).scalar()
            # types breakdown
            rows = session.query(Filing.form_type, func.count(Filing.id)).group_by(Filing.form_type).all()
            stats["types"] = [(ft or "Unknown", cnt) for ft, cnt in rows]
        except SQLAlchemyError as e:
            logger.error(f"Error getting filings stats: {str(e)}")
        finally:
            session.close()
        return stats

    def get_swaps_by_counterparty_id(self, counterparty_id: int) -> List[Dict[str, Any]]:
        """Get all swaps for a specific counterparty by their ID."""
        session = self.Session()
        try:
            swaps = session.query(Swap).filter_by(counterparty_id=counterparty_id).all()
            return [s.to_dict() for s in swaps]
        except SQLAlchemyError as e:
            logger.error(f"Error getting swaps by counterparty ID: {str(e)}")
            return []
        finally:
            session.close()

    def get_swaps_by_security_id(self, security_id: int) -> List[Dict[str, Any]]:
        """Get all swaps related to a specific reference security by its ID."""
        session = self.Session()
        try:
            swaps = session.query(Swap).join(UnderlyingInstrument).filter(UnderlyingInstrument.security_id == security_id).all()
            return [s.to_dict() for s in swaps]
        except SQLAlchemyError as e:
            logger.error(f"Error getting swaps by security ID: {str(e)}")
            return []
        finally:
            session.close()
    
    # SEC Database methods
    def save_company(self, company_info: CompanyInfo) -> bool:
        """Save company information to the database using SQLAlchemy."""
        session = self.Session()
        try:
            primary = company_info.primary_identifiers
            company = session.query(Company).filter_by(cik=primary.cik).first()

            if company:
                company.name = primary.name
                company.description = primary.description
                company.updated_at = datetime.utcnow()
            else:
                company = Company(
                    cik=primary.cik,
                    name=primary.name,
                    description=primary.description
                )
                session.add(company)

            session.query(AltTicker).filter_by(company_cik=primary.cik).delete()
            session.query(RelatedEntity).filter_by(company_cik=primary.cik).delete()

            if hasattr(primary, 'tickers') and primary.tickers:
                for ticker_data in primary.tickers:
                    ticker = AltTicker(
                        company_cik=primary.cik,
                        symbol=ticker_data['symbol'],
                        exchange=ticker_data.get('exchange'),
                        security_type=ticker_data.get('security_type')
                    )
                    session.add(ticker)

            for entity_data in company_info.related_entities:
                entity = RelatedEntity(
                    company_cik=primary.cik,
                    name=entity_data.name,
                    cik=entity_data.cik,
                    description=entity_data.description,
                    relationship_type=entity_data.relationship_type
                )
                session.add(entity)

            session.commit()
            return True

        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Error saving company {primary.cik}: {str(e)}")
            return False
        finally:
            session.close()
