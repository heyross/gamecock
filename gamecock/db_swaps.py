"""Database handler for swaps data."""
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from sqlalchemy import create_engine, Column, Integer, String, Float, Date, DateTime, Text, ForeignKey, JSON, Boolean, func, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, Session
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)

Base = declarative_base()

class Swap(Base):
    """Swap contract model."""
    __tablename__ = 'swaps'
    
    id = Column(Integer, primary_key=True)
    contract_id = Column(String(100), unique=True, nullable=False)
    counterparty = Column(String(255), nullable=False)
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
    
    # Relationships
    obligations = relationship("SwapObligation", back_populates="swap", cascade="all, delete-orphan")
    analysis = relationship("SwapAnalysis", back_populates="swap", uselist=False, cascade="all, delete-orphan")
    underlying_instruments = relationship("UnderlyingInstrument", back_populates="swap", cascade="all, delete-orphan")
    
    def to_dict(self):
        """Convert swap to dictionary."""
        return {
            'id': self.id,
            'contract_id': self.contract_id,
            'counterparty': self.counterparty,
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
    
    # Relationships
    swap = relationship("Swap", back_populates="obligations")
    triggers = relationship("ObligationTrigger", back_populates="obligation", cascade="all, delete-orphan")
    
    def to_dict(self):
        """Convert obligation to dictionary."""
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
    
    # Relationships
    swap = relationship("Swap", back_populates="analysis")
    
    def to_dict(self):
        """Convert analysis to dictionary."""
        return {
            'id': self.id,
            'swap_id': self.swap_id,
            'analysis_text': self.analysis_text,
            'risk_score': self.risk_score,
            'key_risks': self.key_risks,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }

class UnderlyingInstrument(Base):
    """Represents an underlying instrument in a swap contract."""
    __tablename__ = 'underlying_instruments'
    
    id = Column(Integer, primary_key=True)
    swap_id = Column(Integer, ForeignKey('swaps.id', ondelete='CASCADE'), nullable=False)
    instrument_type = Column(String(50), nullable=False)
    identifier = Column(String(100), nullable=False)
    description = Column(Text, nullable=True)
    quantity = Column(Float, nullable=True)
    notional_amount = Column(Float, nullable=True)
    currency = Column(String(10), nullable=True, default='USD')
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    # Relationships
    swap = relationship("Swap", back_populates="underlying_instruments")
    
    def to_dict(self):
        """Convert to dictionary."""
        return {
            'id': self.id,
            'swap_id': self.swap_id,
            'instrument_type': self.instrument_type,
            'identifier': self.identifier,
            'description': self.description,
            'quantity': self.quantity,
            'notional_amount': self.notional_amount,
            'currency': self.currency,
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
    
    # Relationships
    obligation = relationship("SwapObligation", back_populates="triggers")
    
    def to_dict(self):
        """Convert to dictionary."""
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

class SwapsDatabase:
    """Database handler for swaps data."""
    
    def __init__(self, db_url: str = "sqlite:///swaps.db"):
        """Initialize the database connection.
        
        Args:
            db_url: Database connection URL
        """
        self.engine = create_engine(db_url)
        self.Session = sessionmaker(bind=self.engine)
        self._create_tables()
        self._create_view()
        
    def _create_tables(self):
        """Create database tables if they don't exist."""
        Base.metadata.create_all(self.engine)
    
    def _create_view(self):
        """Create the database view for swap obligations."""
        view_sql = """
        CREATE VIEW IF NOT EXISTS vw_swap_obligations AS
        SELECT 
            s.id AS swap_id,
            s.contract_id,
            s.counterparty,
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
            ui.identifier AS instrument_identifier,
            ui.description AS instrument_description,
            ui.quantity,
            ui.notional_amount AS instrument_notional,
            ot.trigger_type,
            ot.trigger_condition,
            ot.description AS trigger_description
        FROM 
            swaps s
        LEFT JOIN 
            swap_obligations o ON s.id = o.swap_id
        LEFT JOIN 
            underlying_instruments ui ON s.id = ui.swap_id
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
    
    def save_swap(self, swap_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Save a swap contract to the database.
        
        Args:
            swap_data: Dictionary containing swap data
            
        Returns:
            Dictionary containing the saved swap data or None if failed
        """
        session = self.Session()
        try:
            # Check if swap already exists
            existing_swap = session.query(Swap).filter_by(contract_id=swap_data['contract_id']).first()
            
            # Ensure date fields are actual date objects
            for date_field in ['effective_date', 'maturity_date']:
                if date_field in swap_data and isinstance(swap_data[date_field], str):
                    swap_data[date_field] = datetime.strptime(swap_data[date_field], '%Y-%m-%d').date()
            
            if existing_swap:
                # Update existing swap
                for key, value in swap_data.items():
                    if hasattr(existing_swap, key) and key != 'id':
                        setattr(existing_swap, key, value)
                existing_swap.updated_at = datetime.utcnow()
                swap = existing_swap
            else:
                # Create new swap
                swap = Swap(**swap_data)
                session.add(swap)
            
            session.commit()
            return existing_swap.to_dict() if existing_swap else swap.to_dict()
            
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
            # Check if analysis already exists
            analysis = session.query(SwapAnalysis).filter_by(swap_id=swap_id).first()
            
            if analysis:
                # Update existing analysis
                for key, value in analysis_data.items():
                    if hasattr(analysis, key) and key != 'id':
                        setattr(analysis, key, value)
                analysis.updated_at = datetime.utcnow()
            else:
                # Create new analysis
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
            
            # Add analysis if exists
            if swap.analysis:
                result['analysis'] = swap.analysis.to_dict()
            
            # Add obligations
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
            swaps = session.query(Swap).filter_by(counterparty=counterparty).all()
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
            instruments = session.query(UnderlyingInstrument).filter_by(identifier=instrument_identifier).all()
            obligations = []
            for instrument in instruments:
                swap = instrument.swap
                for obligation in swap.obligations:
                    obligation_dict = obligation.to_dict()
                    obligation_dict['swap_contract_id'] = swap.contract_id
                    obligation_dict['counterparty'] = swap.counterparty
                    obligation_dict['instrument_type'] = instrument.instrument_type
                    obligation_dict['instrument_identifier'] = instrument.identifier
                    obligations.append(obligation_dict)
            return obligations
        except SQLAlchemyError as e:
            logger.error(f"Error getting obligations by instrument: {str(e)}")
            return []
        finally:
            session.close()
