"""Database handler for swaps data."""
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from sqlalchemy import create_engine, Column, Integer, String, Float, Date, DateTime, Text, ForeignKey, JSON, func
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
        
    def _create_tables(self):
        """Create database tables if they don't exist."""
        Base.metadata.create_all(self.engine)
    
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
            
            if existing_swap:
                # Update existing swap
                for key, value in swap_data.items():
                    if hasattr(existing_swap, key) and key != 'id':
                        setattr(existing_swap, key, value)
                existing_swap.updated_at = datetime.utcnow()
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
