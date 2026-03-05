
from datetime import datetime
from sqlalchemy import Column, String, Boolean, Integer, DateTime
from .base import Base

class CompanySchedulerConfig(Base):
    __tablename__ = "company_scheduler_config"

    company_name   = Column(String(255), primary_key=True, nullable=False)
    enabled        = Column(Boolean,  default=False,   nullable=False)
    interval       = Column(String(20), default="hourly", nullable=False)
    value          = Column(Integer,  default=1,        nullable=False)
    time           = Column(String(10), default="09:00", nullable=False)
    last_sync_time = Column(DateTime, nullable=True,    default=None)
    updated_at     = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return (
            f"<CompanySchedulerConfig "
            f"name={self.company_name!r} "
            f"enabled={self.enabled} "
            f"interval={self.interval!r} "
            f"last_sync={self.last_sync_time}>"
        )