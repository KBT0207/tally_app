"""
gui/controllers/company_controller.py
=======================================
Loads and persists per-company scheduler configuration in MySQL.

Table: company_scheduler_config  (see database/models/scheduler_config.py)
  One row per company — upserted on every save.

Replaces the old scheduler_config.json flat-file approach.
"""

from datetime import datetime, timedelta
from typing import Optional

from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.mysql import insert as mysql_insert

from gui.state import AppState, CompanyState
from logging_config import logger


def _get_model():
    """Lazy import to avoid circular deps at module load time."""
    from database.models.scheduler_config import CompanySchedulerConfig
    return CompanySchedulerConfig


class CompanyController:

    def __init__(self, state: AppState):
        self._state = state

    # ─────────────────────────────────────────────────────────────────────────
    #  Load  DB → state
    # ─────────────────────────────────────────────────────────────────────────
    def load_scheduler_config(self):
        """
        Read company_scheduler_config table and apply to matching CompanyState
        objects in state.companies.  Safe to call even if table is empty.
        """
        engine = self._state.db_engine
        if not engine:
            logger.warning("[CompanyController] No DB engine — cannot load scheduler config")
            return

        Model   = _get_model()
        Session = sessionmaker(bind=engine)
        db      = Session()
        try:
            rows = db.query(Model).all()
            for row in rows:
                co = self._state.companies.get(row.company_name)
                if co:
                    co.schedule_enabled  = bool(row.enabled)
                    co.schedule_interval = row.interval or "hourly"
                    co.schedule_value    = int(row.value  or 1)
                    co.schedule_time     = row.time       or "09:00"
            logger.info(f"[CompanyController] Loaded scheduler config for {len(rows)} companies")
        except Exception as e:
            logger.error(f"[CompanyController] Failed to load scheduler config: {e}")
        finally:
            db.close()

    # ─────────────────────────────────────────────────────────────────────────
    #  Save one company  state → DB  (upsert)
    # ─────────────────────────────────────────────────────────────────────────
    def save_one(self, name: str):
        """
        Upsert scheduler config for a single company.
        Uses MySQL  INSERT … ON DUPLICATE KEY UPDATE  for atomic upsert.
        """
        engine = self._state.db_engine
        if not engine:
            logger.warning("[CompanyController] No DB engine — cannot save scheduler config")
            return

        co = self._state.companies.get(name)
        if not co:
            logger.warning(f"[CompanyController] Company not found in state: {name}")
            return

        self._upsert(engine, name, co)

    # ─────────────────────────────────────────────────────────────────────────
    #  Save all companies  state → DB
    # ─────────────────────────────────────────────────────────────────────────
    def save_scheduler_config(self):
        """Upsert scheduler config for every company in state."""
        engine = self._state.db_engine
        if not engine:
            logger.warning("[CompanyController] No DB engine — cannot save scheduler config")
            return

        for name, co in self._state.companies.items():
            self._upsert(engine, name, co)

        logger.info(f"[CompanyController] Saved scheduler config for "
                    f"{len(self._state.companies)} companies")

    # ─────────────────────────────────────────────────────────────────────────
    #  Internal upsert helper
    # ─────────────────────────────────────────────────────────────────────────
    def _upsert(self, engine, name: str, co: CompanyState):
        """
        INSERT … ON DUPLICATE KEY UPDATE for one company row.
        Works correctly whether the row already exists or not.
        """
        Model   = _get_model()
        Session = sessionmaker(bind=engine)
        db      = Session()
        try:
            stmt = (
                mysql_insert(Model)
                .values(
                    company_name = name,
                    enabled      = co.schedule_enabled,
                    interval     = co.schedule_interval,
                    value        = co.schedule_value,
                    time         = co.schedule_time,
                    updated_at   = datetime.utcnow(),
                )
                .on_duplicate_key_update(
                    enabled    = co.schedule_enabled,
                    interval   = co.schedule_interval,
                    value      = co.schedule_value,
                    time       = co.schedule_time,
                    updated_at = datetime.utcnow(),
                )
            )
            db.execute(stmt)
            db.commit()
            logger.debug(f"[CompanyController] Upserted scheduler config for: {name}")
        except Exception as e:
            db.rollback()
            logger.error(f"[CompanyController] Failed to save config for {name}: {e}")
        finally:
            db.close()

    # ─────────────────────────────────────────────────────────────────────────
    #  Compute next run time string for display  (no DB access needed)
    # ─────────────────────────────────────────────────────────────────────────
    @staticmethod
    def next_run_label(co: CompanyState) -> str:
        """Return a human-readable 'Next run: ...' string for the scheduler UI."""
        if not co.schedule_enabled:
            return "—"

        now = datetime.now()

        if co.schedule_interval == "minutes":
            return (now + timedelta(minutes=co.schedule_value)).strftime("%d %b %Y  %H:%M")

        elif co.schedule_interval == "hourly":
            return (now + timedelta(hours=co.schedule_value)).strftime("%d %b %Y  %H:%M")

        elif co.schedule_interval == "daily":
            try:
                h, m   = map(int, co.schedule_time.split(":"))
                target = now.replace(hour=h, minute=m, second=0, microsecond=0)
                if target <= now:
                    target += timedelta(days=1)
                return target.strftime("%d %b %Y  %H:%M")
            except Exception:
                return "—"

        return "—"