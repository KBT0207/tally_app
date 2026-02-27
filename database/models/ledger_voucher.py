from sqlalchemy import Column, String, BigInteger, DateTime, Float, Date, Text
from sqlalchemy.sql import func
from .base import Base


class _LedgerVoucherMixin:
    id             = Column(BigInteger,   primary_key=True, autoincrement=True)
    company_name   = Column(String(255),  nullable=False, index=True)
    date           = Column(Date,         nullable=True,  index=True)
    voucher_type   = Column(String(100),  nullable=True)
    voucher_number = Column(String(100),  nullable=True)
    reference      = Column(String(255),  nullable=True)
    ledger_name    = Column(String(255),  nullable=True,  index=True)
    amount         = Column(Float,        nullable=True,  default=0.0)
    amount_type    = Column(String(10),   nullable=True)
    currency       = Column(String(10),   nullable=True,  default='INR')
    exchange_rate  = Column(Float,        nullable=True,  default=1.0)
    narration      = Column(Text,         nullable=True)
    guid           = Column(String(255),  nullable=False, index=True)
    alter_id       = Column(BigInteger,   nullable=False, default=0)
    master_id      = Column(String(255),  nullable=True)
    change_status  = Column(String(50),   nullable=True)
    is_deleted     = Column(String(3),    nullable=False, default='No')
    created_at     = Column(DateTime,     server_default=func.now())
    updated_at     = Column(DateTime,     server_default=func.now(), onupdate=func.now())


class ReceiptVoucher(_LedgerVoucherMixin, Base):
    __tablename__ = 'receipt_vouchers'

    def __repr__(self):
        return (
            f"<ReceiptVoucher("
            f"company='{self.company_name}', "
            f"number='{self.voucher_number}', "
            f"ledger='{self.ledger_name}'"
            f")>"
        )


class PaymentVoucher(_LedgerVoucherMixin, Base):
    __tablename__ = 'payment_vouchers'

    def __repr__(self):
        return (
            f"<PaymentVoucher("
            f"company='{self.company_name}', "
            f"number='{self.voucher_number}', "
            f"ledger='{self.ledger_name}'"
            f")>"
        )


class JournalVoucher(_LedgerVoucherMixin, Base):
    __tablename__ = 'journal_vouchers'

    def __repr__(self):
        return (
            f"<JournalVoucher("
            f"company='{self.company_name}', "
            f"number='{self.voucher_number}', "
            f"ledger='{self.ledger_name}'"
            f")>"
        )


class ContraVoucher(_LedgerVoucherMixin, Base):
    __tablename__ = 'contra_vouchers'

    def __repr__(self):
        return (
            f"<ContraVoucher("
            f"company='{self.company_name}', "
            f"number='{self.voucher_number}', "
            f"ledger='{self.ledger_name}'"
            f")>"
        )