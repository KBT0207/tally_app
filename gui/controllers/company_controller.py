"""
gui/controllers/company_controller.py
=======================================
Loads and persists per-company scheduler configuration in MySQL.

Phase 2 fix:
  - next_run_label() now accepts an optional `scheduler_controller` argument
    and reads the true next_run_time directly from the live APScheduler job.
  - Falls back to computed estimate only if scheduler is unavailable.
  - This fixes the bug where the display always showed "from now + interval"
    instead of the actual scheduled time.
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
        objects in state.companies.

        Loads ALL fields including last_sync_time so the scheduler page
        can show "Last sync" even when Tally is completely closed.
        Previously last_sync_time was only held in memory — lost on restart
        and blank whenever Tally was not running.
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

                    # Load last_sync_time from DB so it shows even when Tally is closed.
                    # This field is written by SyncController after every successful sync.
                    if hasattr(row, 'last_sync_time') and row.last_sync_time:
                        co.last_sync_time = row.last_sync_time

            logger.info(f"[CompanyController] Loaded scheduler config for {len(rows)} companies")
        except Exception as e:
            logger.error(f"[CompanyController] Failed to load scheduler config: {e}")
        finally:
            db.close()

    # ─────────────────────────────────────────────────────────────────────────
    #  Save one company  state → DB  (upsert)
    # ─────────────────────────────────────────────────────────────────────────
    def save_one(self, name: str):
        """Upsert scheduler config for a single company."""
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
        Upsert company scheduler config into DB.
        Saves last_sync_time so it persists across app restarts and
        is visible on the scheduler page even when Tally is closed.
        """
        Model   = _get_model()
        Session = sessionmaker(bind=engine)
        db      = Session()
        try:
            # Build values dict — only include last_sync_time if it exists
            values = dict(
                company_name = name,
                enabled      = co.schedule_enabled,
                interval     = co.schedule_interval,
                value        = co.schedule_value,
                time         = co.schedule_time,
                updated_at   = datetime.utcnow(),
            )
            update_vals = dict(
                enabled    = co.schedule_enabled,
                interval   = co.schedule_interval,
                value      = co.schedule_value,
                time       = co.schedule_time,
                updated_at = datetime.utcnow(),
            )

            # Include last_sync_time if available on this CompanyState
            last_sync = getattr(co, 'last_sync_time', None)
            if last_sync:
                values['last_sync_time']     = last_sync
                update_vals['last_sync_time'] = last_sync

            stmt = (
                mysql_insert(Model)
                .values(**values)
                .on_duplicate_key_update(**update_vals)
            )
            db.execute(stmt)
            db.commit()
            logger.debug(f"[CompanyController] Upserted config for: {name}")
        except Exception as e:
            db.rollback()
            logger.error(f"[CompanyController] Failed to save config for {name}: {e}")
        finally:
            db.close()

    # ─────────────────────────────────────────────────────────────────────────
    #  Phase 2 fix: next run time — reads from live APScheduler job
    # ─────────────────────────────────────────────────────────────────────────
    @staticmethod
    def next_run_label(
        co: CompanyState,
        scheduler_controller=None,   # pass SchedulerController instance if available
    ) -> str:
        """
        Return a human-readable 'Next run: ...' string for the scheduler UI.

        Phase 2: If scheduler_controller is provided, reads the TRUE next_run_time
        from the live APScheduler job. This is accurate because APScheduler tracks
        the actual scheduled fire time, not just "now + interval".

        Falls back to an estimated computed time if:
          - scheduler_controller is None (APScheduler not running)
          - job not found (company not scheduled yet)
        """
        if not co.schedule_enabled:
            return "—"

        # ── Primary: read from live APScheduler job ───────────────────────────
        if scheduler_controller is not None:
            try:
                next_run = scheduler_controller.get_next_run(co.name)
                if next_run is not None:
                    # APScheduler returns timezone-aware datetime — convert to local naive
                    try:
                        # Strip timezone for display
                        next_local = next_run.replace(tzinfo=None)
                    except Exception:
                        next_local = next_run
                    return next_local.strftime("%d %b %Y  %H:%M")
            except Exception as e:
                logger.debug(f"[CompanyController] Could not get next_run from scheduler: {e}")

        # ── Fallback: estimate from current time + interval ───────────────────
        # Used when APScheduler is unavailable (no APScheduler installed,
        # scheduler not started yet, or job not yet registered)
        return CompanyController._estimate_next_run(co)

    @staticmethod
    def _estimate_next_run(co: CompanyState) -> str:
        """
        Estimate next run time from current time + configured interval.
        This is only an approximation — actual APScheduler time may differ.
        """
        if not co.schedule_enabled:
            return "—"

        now = datetime.now()

        if co.schedule_interval == "minutes":
            return (now + timedelta(minutes=max(1, co.schedule_value))).strftime("%d %b %Y  %H:%M")

        elif co.schedule_interval == "hourly":
            return (now + timedelta(hours=max(1, co.schedule_value))).strftime("%d %b %Y  %H:%M")

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