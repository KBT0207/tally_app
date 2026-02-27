from datetime import datetime
from sqlalchemy import (
    Column, String, Boolean, Integer, DateTime,
    UniqueConstraint,
)
from .base import Base

class CompanySchedulerConfig(Base):

    __tablename__ = "company_scheduler_config"

    company_name = Column(String(255), primary_key=True, nullable=False)
    enabled      = Column(Boolean,     nullable=False, default=False)
    interval     = Column(String(20),  nullable=False, default="hourly")
    value        = Column(Integer,     nullable=False, default=1)
    time         = Column(String(10),  nullable=False, default="09:00")
    updated_at   = Column(DateTime,    nullable=False, default=datetime.utcnow,
                          onupdate=datetime.utcnow)

    def __repr__(self):
        return (
            f"<CompanySchedulerConfig "
            f"company={self.company_name!r} "
            f"enabled={self.enabled} "
            f"interval={self.interval} every {self.value} "
            f"time={self.time}>"
        )