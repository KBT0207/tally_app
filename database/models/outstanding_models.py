from sqlalchemy import Column, String, BigInteger, DateTime, Float, Date, Text
from sqlalchemy.sql import func
from .base import Base


class _OutstandingMixin:
    id             = Column(BigInteger,  primary_key=True, autoincrement=True)
    company_name   = Column(String(255), nullable=False, index=True)
    party_name     = Column(String(255), nullable=True,  index=True)
    voucher_number = Column(String(100), nullable=True,  index=True)
    voucher_type   = Column(String(100), nullable=True)
    bill_name      = Column(String(255), nullable=True)
    bill_type      = Column(String(100), nullable=True)
    date           = Column(Date,        nullable=True,  index=True)
    bill_date      = Column(Date,        nullable=True)
    due_date       = Column(Date,        nullable=True,  index=True)
    reference      = Column(String(255), nullable=True)
    currency       = Column(String(10),  nullable=True,  default='INR')
    exchange_rate  = Column(Float,       nullable=True,  default=1.0)
    amount         = Column(Float,       nullable=True,  default=0.0)
    narration      = Column(Text,        nullable=True)
    material_centre= Column(String(255), nullable=True)
    created_at     = Column(DateTime,    server_default=func.now())
    updated_at     = Column(DateTime,    server_default=func.now(), onupdate=func.now())


class DebtorOutstanding(_OutstandingMixin, Base):
    """Sundry Debtors — Receivables outstanding."""
    __tablename__ = 'debtor_outstanding'

    def __repr__(self):
        return (
            f"<DebtorOutstanding("
            f"company='{self.company_name}', "
            f"party='{self.party_name}', "
            f"bill='{self.bill_name}', "
            f"amount={self.amount}"
            f")>"
        )


# class CreditorOutstanding(_OutstandingMixin, Base):
#     """Sundry Creditors — Payables outstanding."""
#     __tablename__ = 'creditor_outstanding'

#     def __repr__(self):
#         return (
#             f"<CreditorOutstanding("
#             f"company='{self.company_name}', "
#             f"party='{self.party_name}', "
#             f"bill='{self.bill_name}', "
#             f"amount={self.amount}"
#             f")>"
#         )
