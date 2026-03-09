"""
database/models/tally_settings.py
===================================
Stores global Tally configuration — Tally.exe path and image filenames
used by PyAutoGUI automation (Phase 2).

Always a single row (id = 1). Upserted on save, never inserted twice.
"""

from sqlalchemy import Column, Integer, String, DateTime
from datetime import datetime
from .base import Base


class TallySettings(Base):
    __tablename__ = 'tally_settings'

    id          = Column(Integer, primary_key=True, default=1)

    # ── Tally executable ──────────────────────────────────────────────────────
    exe_path    = Column(String(500), nullable=True)   # e.g. C:\TallyPrime\tally.exe

    # ── PyAutoGUI image filenames (stored in assets/ folder) ─────────────────
    # These are just filenames — TallyLauncher builds the full path at runtime.
    # Default values match the filenames already in your assets/ folder.
    image_gateway      = Column(String(255), default='tally_gateway_screen.png')
    image_search_box   = Column(String(255), default='tally_company_search_box.png')
    image_username     = Column(String(255), default='tally_username_field.png')
    image_password     = Column(String(255), default='tally_password_field.png')
    image_select_title = Column(String(255), default='tally_select_company_title.png')
    image_change_path  = Column(String(255), default='tally_change_path_btn.png')
    image_remote_tab   = Column(String(255), default='tally_remote_tab.png')
    image_tds_field    = Column(String(255), default='tally_tds_field.png')
    image_data_server  = Column(String(255), default='tally_dataserver_image.png')   # TDS: Data Server button
    image_local_path   = Column(String(255), default='tally_local_path_image.png')   # TDS: path input field

    updated_at  = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return f"<TallySettings(exe_path={self.exe_path})>"