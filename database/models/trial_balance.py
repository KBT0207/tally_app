from sqlalchemy import Column, String, BigInteger, DateTime, Float, Date
from sqlalchemy.sql import func
from .base import Base


class TrialBalance(Base):
    __tablename__ = 'trial_balance'

    id               = Column(BigInteger,  primary_key=True, autoincrement=True)
    company_name     = Column(String(255), nullable=False, index=True)
    ledger_name      = Column(String(255), nullable=False, index=True)
    parent_group     = Column(String(255), nullable=True)
    opening_balance  = Column(Float,       nullable=True, default=0.0)
    net_transactions = Column(Float,       nullable=True, default=0.0)
    closing_balance  = Column(Float,       nullable=True, default=0.0)
    start_date       = Column(Date,        nullable=True)
    end_date         = Column(Date,        nullable=True)
    guid             = Column(String(255), nullable=False, index=True)
    alter_id         = Column(BigInteger,  nullable=False, default=0)
    master_id        = Column(String(255), nullable=True)
    created_at       = Column(DateTime,    server_default=func.now())
    updated_at       = Column(DateTime,    server_default=func.now(), onupdate=func.now())

    def __repr__(self):
        return (
            f"<TrialBalance("
            f"company='{self.company_name}', "
            f"ledger='{self.ledger_name}', "
            f"closing={self.closing_balance}"
            f")>"
        )