from sqlalchemy import (
    Column, String, Numeric, Integer, DateTime, UniqueConstraint, Index
)
from sqlalchemy.sql import func
from .base import Base


class Item(Base):
    __tablename__ = 'tally_items_master'

    id = Column(Integer, primary_key=True, autoincrement=True)

    company_name = Column(String(255), nullable=False, index=True)

    item_name = Column(String(500), nullable=False, default='')
    parent_group = Column(String(255), nullable=False, default='')
    category = Column(String(255), nullable=False, default='')
    base_units = Column(String(100), nullable=False, default='')
    gst_type_of_supply = Column(String(100), nullable=False, default='')

    hsn_code = Column(String(20), nullable=False, default='', index=True)
    gst_applicable_from = Column(String(20), nullable=True)
    taxability = Column(String(50), nullable=True)

    cgst_rate = Column(Numeric(10, 2), nullable=False, default=0)
    sgst_rate = Column(Numeric(10, 2), nullable=False, default=0)
    igst_rate = Column(Numeric(10, 2), nullable=False, default=0)
    cess_rate = Column(Numeric(10, 2), nullable=False, default=0)

    opening_balance = Column(Numeric(18, 4), nullable=False, default=0)
    opening_rate = Column(Numeric(18, 4), nullable=False, default=0)
    opening_value = Column(Numeric(18, 4), nullable=False, default=0)

    entered_by = Column(String(255), nullable=False, default='')
    is_deleted = Column(String(10), nullable=False, default='No')

    guid = Column(String(100), nullable=False, default='')
    remote_alt_guid = Column(String(100), nullable=False, default='')
    alter_id = Column(Integer, nullable=False, default=0, index=True)
    material_centre = Column(String(255), nullable=True)

    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    __table_args__ = (
        UniqueConstraint('company_name', 'guid', name='uq_item_company_guid'),
        UniqueConstraint('company_name', 'item_name', name='uq_item_company_name'),
        Index('ix_item_company_alter_id', 'company_name', 'alter_id'),
        Index('ix_item_parent_group', 'company_name', 'parent_group'),
        Index('ix_item_hsn', 'company_name', 'hsn_code'),
    )

    def __repr__(self) -> str:
        return (
            f"<Item company={self.company_name!r} "
            f"item={self.item_name!r} "
            f"hsn={self.hsn_code!r} "
            f"alter_id={self.alter_id}>"
        )