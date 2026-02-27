from sqlalchemy import Column, String, BigInteger, DateTime, Boolean
from sqlalchemy.sql import func
from .base import Base


class SyncState(Base):
    __tablename__ = 'sync_state'

    company_name      = Column(String(255), primary_key=True, nullable=False)
    voucher_type      = Column(String(100), primary_key=True, nullable=False)
    last_alter_id     = Column(BigInteger,  nullable=False, default=0)
    is_initial_done   = Column(Boolean,     nullable=False, default=False)
    last_synced_month = Column(String(6),   nullable=True)
    last_sync_time    = Column(DateTime,    nullable=True)
    created_at        = Column(DateTime,    server_default=func.now())
    updated_at        = Column(DateTime,    server_default=func.now(), onupdate=func.now())

    def __repr__(self):
        return (
            f"<SyncState("
            f"company='{self.company_name}', "
            f"voucher='{self.voucher_type}', "
            f"last_alter_id={self.last_alter_id}, "
            f"last_synced_month={self.last_synced_month}"
            f")>"
        )