"""Add swaps tables

Revision ID: 0002
Revises: 0001
Create Date: 2025-09-07 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import sqlite

# revision identifiers, used by Alembic.
revision = '0002'
down_revision = '0001'
branch_labels = None
depends_on = None

def upgrade():
    # Create swaps table
    op.create_table(
        'swaps',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('contract_id', sa.String(100), nullable=False, unique=True),
        sa.Column('counterparty', sa.String(255), nullable=False),
        sa.Column('reference_entity', sa.String(255), nullable=False),
        sa.Column('notional_amount', sa.Float(), nullable=False),
        sa.Column('currency', sa.String(10), nullable=False, default='USD'),
        sa.Column('effective_date', sa.Date(), nullable=False),
        sa.Column('maturity_date', sa.Date(), nullable=False),
        sa.Column('swap_type', sa.String(50), nullable=True),
        sa.Column('payment_frequency', sa.String(50), nullable=True),
        sa.Column('fixed_rate', sa.Float(), nullable=True),
        sa.Column('floating_rate_index', sa.String(100), nullable=True),
        sa.Column('floating_rate_spread', sa.Float(), nullable=True),
        sa.Column('collateral_terms', sa.JSON(), nullable=True),
        sa.Column('additional_terms', sa.JSON(), nullable=True),
        sa.Column('created_at', sa.DateTime(), server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.func.now(), onupdate=sa.func.now()),
        sa.PrimaryKeyConstraint('id'),
        sa.Index('idx_swaps_contract_id', 'contract_id'),
        sa.Index('idx_swaps_reference_entity', 'reference_entity'),
        sa.Index('idx_swaps_counterparty', 'counterparty'),
        sa.Index('idx_swaps_maturity_date', 'maturity_date')
    )
    
    # Create swap_obligations table
    op.create_table(
        'swap_obligations',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('swap_id', sa.Integer(), sa.ForeignKey('swaps.id', ondelete='CASCADE'), nullable=False),
        sa.Column('obligation_type', sa.String(50), nullable=False),
        sa.Column('amount', sa.Float(), nullable=False),
        sa.Column('currency', sa.String(10), nullable=False, default='USD'),
        sa.Column('due_date', sa.Date(), nullable=True),
        sa.Column('status', sa.String(50), nullable=True, default='pending'),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(), server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.func.now(), onupdate=sa.func.now()),
        sa.PrimaryKeyConstraint('id'),
        sa.Index('idx_swap_obligations_swap_id', 'swap_id'),
        sa.Index('idx_swap_obligations_due_date', 'due_date'),
        sa.Index('idx_swap_obligations_status', 'status')
    )
    
    # Create swap_analysis table
    op.create_table(
        'swap_analysis',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('swap_id', sa.Integer(), sa.ForeignKey('swaps.id', ondelete='CASCADE'), nullable=False),
        sa.Column('analysis_text', sa.Text(), nullable=True),
        sa.Column('risk_score', sa.Float(), nullable=True),
        sa.Column('key_risks', sa.JSON(), nullable=True),
        sa.Column('created_at', sa.DateTime(), server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.func.now(), onupdate=sa.func.now()),
        sa.PrimaryKeyConstraint('id'),
        sa.Index('idx_swap_analysis_swap_id', 'swap_id')
    )

def downgrade():
    op.drop_table('swap_analysis')
    op.drop_table('swap_obligations')
    op.drop_table('swaps')
