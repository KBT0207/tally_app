from sqlalchemy import Column, String, BigInteger, DateTime, Text
from sqlalchemy.sql import func
from .base import Base


class Ledger(Base):
    __tablename__ = 'ledgers'

    id                    = Column(BigInteger,  primary_key=True, autoincrement=True)
    company_name          = Column(String(255), nullable=False, index=True)
    ledger_name           = Column(String(255), nullable=False, index=True)
    alias                 = Column(String(255), nullable=True)
    alias_2               = Column(String(255), nullable=True)
    alias_3               = Column(String(255), nullable=True)
    parent_group          = Column(String(255), nullable=True)
    contact_person        = Column(String(255), nullable=True)
    email                 = Column(String(255), nullable=True)
    phone                 = Column(String(100), nullable=True)
    mobile                = Column(String(100), nullable=True)
    fax                   = Column(String(100), nullable=True)
    website               = Column(String(500), nullable=True)
    address_line_1        = Column(Text,        nullable=True)
    address_line_2        = Column(Text,        nullable=True)
    address_line_3        = Column(Text,        nullable=True)
    pincode               = Column(String(100), nullable=True)
    state                 = Column(String(255), nullable=True)
    country               = Column(String(255), nullable=True)
    opening_balance       = Column(String(100), nullable=True)
    credit_limit          = Column(String(100), nullable=True)
    bill_credit_period    = Column(String(100), nullable=True)
    pan                   = Column(String(100), nullable=True)
    gstin                 = Column(String(100), nullable=True, index=True)
    gst_registration_type = Column(String(255), nullable=True)
    vat_tin               = Column(String(100), nullable=True)
    sales_tax_number      = Column(String(100), nullable=True)
    bank_account_holder   = Column(String(255), nullable=True)
    ifsc_code             = Column(String(100), nullable=True)
    bank_branch           = Column(String(255), nullable=True)
    swift_code            = Column(String(100), nullable=True)
    bank_iban             = Column(String(100), nullable=True)
    export_import_code    = Column(String(100), nullable=True)
    msme_reg_number       = Column(String(100), nullable=True)
    is_bill_wise_on       = Column(String(10),  nullable=True)
    is_deleted            = Column(String(10),  nullable=True, default='No')
    created_date          = Column(String(20),  nullable=True)
    altered_on            = Column(String(20),  nullable=True)
    guid                  = Column(String(255), nullable=False, index=True)
    alter_id              = Column(BigInteger,  nullable=False, default=0)
    created_at            = Column(DateTime,    server_default=func.now())
    updated_at            = Column(DateTime,    server_default=func.now(), onupdate=func.now())

    def __repr__(self):
        return f"<Ledger(company='{self.company_name}', name='{self.ledger_name}', guid='{self.guid}')>"