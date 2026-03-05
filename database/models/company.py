from sqlalchemy import Column, String, Date, DateTime
from .base import Base
from datetime import datetime


class Company(Base):
    __tablename__ = 'companies'

    guid             = Column(String(255), nullable=False)
    name             = Column(String(255), primary_key=True, index=True)
    formal_name      = Column(String(255), nullable=True)
    company_number   = Column(String(20),  nullable=True)
    starting_from    = Column(Date,        nullable=True)
    books_from       = Column(Date,        nullable=True)
    audited_upto     = Column(Date,        nullable=True)
    tally_username   = Column(String(255), nullable=True)
    tally_password   = Column(String(255), nullable=True)

    # ── Per-company Tally data location (Phase 1) ─────────────────────────────
    # company_type: "local" | "remote" | "tds"
    company_type     = Column(String(20),  nullable=True, default='local')
    # Local / remote mapped drive path  e.g. C:\TallyData\CompanyA
    data_path        = Column(String(500), nullable=True)
    # TDS server IP or hostname         e.g. 192.168.1.10
    tds_path         = Column(String(255), nullable=True)
    # Network drive letter              e.g. Z:
    drive_letter     = Column(String(5),   nullable=True)

    created_at       = Column(DateTime,    default=datetime.utcnow)
    updated_at       = Column(DateTime,    default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return f"<Company(name={self.name}, guid={self.guid})>"