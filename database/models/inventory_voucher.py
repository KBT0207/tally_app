from sqlalchemy import Column, String, BigInteger, DateTime, Float, Date, Text
from sqlalchemy.sql import func
from .base import Base


class _InventoryVoucherMixin:
    id               = Column(BigInteger,   primary_key=True, autoincrement=True)
    company_name     = Column(String(255),  nullable=False, index=True)
    date             = Column(Date,         nullable=True,  index=True)
    voucher_number   = Column(String(100),  nullable=True)
    reference        = Column(String(255),  nullable=True)
    voucher_type     = Column(String(100),  nullable=True)
    party_name       = Column(String(255),  nullable=True,  index=True)
    gst_number       = Column(String(50),   nullable=True)
    e_invoice_number = Column(String(255),  nullable=True)
    eway_bill        = Column(String(255),  nullable=True)
    item_name        = Column(String(255),  nullable=True,  index=True)
    quantity         = Column(Float,        nullable=True,  default=0.0)
    unit             = Column(String(50),   nullable=True)
    alt_qty          = Column(Float,        nullable=True,  default=0.0)
    alt_unit         = Column(String(50),   nullable=True)
    batch_no         = Column(String(255),  nullable=True)
    mfg_date         = Column(String(20),   nullable=True)
    exp_date         = Column(String(20),   nullable=True)
    hsn_code         = Column(String(50),   nullable=True)
    gst_rate         = Column(Float,        nullable=True,  default=0.0)
    rate             = Column(Float,        nullable=True,  default=0.0)
    amount           = Column(Float,        nullable=True,  default=0.0)
    discount         = Column(Float,        nullable=True,  default=0.0)
    cgst_amt         = Column(Float,        nullable=True,  default=0.0)
    sgst_amt         = Column(Float,        nullable=True,  default=0.0)
    igst_amt         = Column(Float,        nullable=True,  default=0.0)
    freight_amt      = Column(Float,        nullable=True,  default=0.0)
    dca_amt          = Column(Float,        nullable=True,  default=0.0)
    cf_amt           = Column(Float,        nullable=True,  default=0.0)
    other_amt        = Column(Float,        nullable=True,  default=0.0)
    total_amt        = Column(Float,        nullable=True,  default=0.0)
    currency         = Column(String(10),   nullable=True,  default='INR')
    exchange_rate    = Column(Float,        nullable=True,  default=1.0)
    narration        = Column(Text,         nullable=True)
    guid             = Column(String(255),  nullable=False, index=True)
    alter_id         = Column(BigInteger,   nullable=False, default=0)
    master_id        = Column(String(255),  nullable=True)
    change_status    = Column(String(50),   nullable=True)
    is_deleted       = Column(String(3),    nullable=False, default='No')
    created_at       = Column(DateTime,     server_default=func.now())
    updated_at       = Column(DateTime,     server_default=func.now(), onupdate=func.now())


class SalesVoucher(_InventoryVoucherMixin, Base):
    __tablename__ = 'sales_vouchers'

    def __repr__(self):
        return (
            f"<SalesVoucher("
            f"company='{self.company_name}', "
            f"number='{self.voucher_number}', "
            f"item='{self.item_name}'"
            f")>"
        )


class PurchaseVoucher(_InventoryVoucherMixin, Base):
    __tablename__ = 'purchase_vouchers'

    def __repr__(self):
        return (
            f"<PurchaseVoucher("
            f"company='{self.company_name}', "
            f"number='{self.voucher_number}', "
            f"item='{self.item_name}'"
            f")>"
        )


class CreditNote(_InventoryVoucherMixin, Base):
    __tablename__ = 'credit_notes'

    def __repr__(self):
        return (
            f"<CreditNote("
            f"company='{self.company_name}', "
            f"number='{self.voucher_number}', "
            f"item='{self.item_name}'"
            f")>"
        )


class DebitNote(_InventoryVoucherMixin, Base):
    __tablename__ = 'debit_notes'

    def __repr__(self):
        return (
            f"<DebitNote("
            f"company='{self.company_name}', "
            f"number='{self.voucher_number}', "
            f"item='{self.item_name}'"
            f")>"
        )