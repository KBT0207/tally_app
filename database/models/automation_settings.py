"""
database/models/automation_settings.py
========================================
Stores PyAutoGUI runtime controls — confidence, delays, timeouts, retries.

Always a single row (id = 1). Adjustable from Settings → Automation tab (Phase 3).
These values are loaded into AppState at startup and read by TallyLauncher.
"""

from sqlalchemy import Column, Integer, Float, DateTime
from datetime import datetime
from .base import Base


class AutomationSettings(Base):
    __tablename__ = 'automation_settings'

    id                = Column(Integer, primary_key=True, default=1)

    # ── PyAutoGUI image matching ───────────────────────────────────────────────
    # How closely the screenshot must match (0.5 = loose, 1.0 = exact pixel match)
    # 0.80 is a safe default for most screens
    confidence        = Column(Float,   default=0.80)

    # ── Timing controls ───────────────────────────────────────────────────────
    # Milliseconds to wait between PyAutoGUI clicks/keystrokes
    click_delay_ms    = Column(Integer, default=500)

    # Seconds to wait for an image to appear before giving up
    wait_timeout_sec  = Column(Integer, default=180)

    # ── Retry controls ────────────────────────────────────────────────────────
    # How many times to retry a failed image search before marking error
    retry_attempts    = Column(Integer, default=3)

    updated_at        = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return (
            f"<AutomationSettings("
            f"confidence={self.confidence}, "
            f"timeout={self.wait_timeout_sec}s, "
            f"retries={self.retry_attempts})>"
        )