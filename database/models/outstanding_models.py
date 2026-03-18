from sqlalchemy import Column, String, BigInteger, DateTime, Float, Date, Text
from sqlalchemy.sql import func
from .base import Base

class _OutstandingMixin:
    id              = Column(BigInteger,  primary_key=True, autoincrement=True)
    company_name    = Column(String(255), nullable=False, index=True)
    party_name      = Column(String(255), nullable=True,  index=True) # From <PARENT>
    parent_group    = Column(String(255), nullable=True,  index=True) # Looked up from Ledger.parent_group
    bill_name       = Column(String(255), nullable=True)             # From <NAME>
    bill_id         = Column(BigInteger,  nullable=True)             # From <BILLID>
    
    # Dates
    bill_date       = Column(Date,        nullable=True)             # From <BILLDATE>
    due_date        = Column(Date,        nullable=True,  index=True) # From <BILLDUEDATE>
    
    # Financials
    currency        = Column(String(10),  nullable=True,  default='INR')
    exchange_rate   = Column(Float,       nullable=True,  default=1.0)
    amount          = Column(Float,       nullable=True,  default=0.0) # From CLOSINGBALANCE
    
    material_centre = Column(String(255), nullable=True)
    created_at      = Column(DateTime,    server_default=func.now())
    updated_at      = Column(DateTime,    server_default=func.now(), onupdate=func.now())

class OutstandingData(_OutstandingMixin, Base):
    """Sundry Debtors — Bill-wise outstanding receivables."""
    __tablename__ = 'outstanding_data'

    def __repr__(self):
        return (
            f"<OutstandingData("
            f"party='{self.party_name}', "
            f"bill='{self.bill_name}', "
            f"amount={self.amount} {self.currency}"
            f")>"
        )