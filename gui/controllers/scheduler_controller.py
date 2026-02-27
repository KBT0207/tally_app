"""
gui/controllers/scheduler_controller.py
=========================================
Manages APScheduler background jobs — one job per company.

Root cause of  "Can't get local object 'create_engine.<locals>.connect'":
  APScheduler's SQLAlchemyJobStore pickles the job's `func` so it can
  persist it in MySQL.  If `func` is an instance method, pickling drags in
  `self` → `self._state` → `self._state.db_engine`, which contains an
  unpicklable SQLAlchemy-internal closure.

Fix: the scheduled function MUST be a plain module-level function that
receives only primitive / picklable arguments (strings, dicts, ints).
It reconstructs everything it needs (engine, SyncController) at call time.
"""

import threading
import queue
import re
from datetime import datetime
from typing import Optional
from urllib.parse import quote_plus

try:
    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.jobstores.sqlalchemy  import SQLAlchemyJobStore
    from apscheduler.triggers.interval     import IntervalTrigger
    from apscheduler.triggers.cron         import CronTrigger
    from apscheduler.events import (
        EVENT_JOB_EXECUTED, EVENT_JOB_ERROR, EVENT_JOB_MISSED
    )
    HAS_APSCHEDULER = True
except ImportError:
    HAS_APSCHEDULER = False

from gui.state import AppState, CompanyState, CompanyStatus
from logging_config import logger


# ─────────────────────────────────────────────────────────────────────────────
#  Module-level job function  ←  the ONLY thing APScheduler pickles
#
#  All arguments must be picklable primitives.
#  We look up the live AppState via a module-level registry rather than
#  capturing it in a closure or instance method.
# ─────────────────────────────────────────────────────────────────────────────

# Registry: maps a small integer key → (AppState, gui_queue)
# Populated by SchedulerController.__init__, cleared on shutdown.
_REGISTRY: dict[int, tuple] = {}
_REGISTRY_NEXT_KEY = 0


def _run_scheduled_sync(registry_key: int, company_name: str):
    """
    Module-level function executed by APScheduler in a background thread.

    Only receives picklable primitives:
      registry_key  — int key into _REGISTRY to retrieve live AppState
      company_name  — str

    Everything else (engine, queues, state) is retrieved at call time
    from the registry so nothing unpicklable ever touches APScheduler's
    pickle/unpickle cycle.
    """
    entry = _REGISTRY.get(registry_key)
    if not entry:
        logger.error(f"[Scheduler] Registry key {registry_key} not found — job orphaned")
        return

    state, gui_queue = entry

    logger.info(f"[Scheduler] Triggered sync for: {company_name}")

    if state.sync_active:
        logger.warning(f"[Scheduler] Skipping {company_name} — sync already running")
        return

    from gui.controllers.sync_controller import SyncController
    import time

    job_q = queue.Queue()

    controller = SyncController(
        state      = state,
        out_queue  = job_q,
        companies  = [company_name],
        sync_mode  = "incremental",
        from_date  = None,
        to_date    = datetime.now().strftime("%Y%m%d"),
        vouchers   = state.voucher_selection,
        sequential = True,
    )
    controller.start()

    # Drain job_q and forward relevant messages to the GUI queue
    while state.sync_active or not job_q.empty():
        try:
            msg = job_q.get(timeout=0.5)
            if msg[0] in ("log", "progress", "status", "done"):
                gui_queue.put(msg)
            elif msg[0] == "all_done":
                gui_queue.put(("scheduler_sync_done", company_name))
                break
        except queue.Empty:
            pass
        time.sleep(0.1)


def _slug(name: str) -> str:
    """Convert company name to a safe APScheduler job ID."""
    return "sync_" + re.sub(r"[^a-zA-Z0-9_]", "_", name)


def _build_url(db_cfg: dict) -> str:
    """Build a pymysql connection URL from the db_config dict."""
    user = quote_plus(str(db_cfg.get("username", "root")))
    pw   = quote_plus(str(db_cfg.get("password", "")))
    host = db_cfg.get("host",     "localhost")
    port = int(db_cfg.get("port", 3306))
    db   = db_cfg.get("database", "tally_db")
    return f"mysql+pymysql://{user}:{pw}@{host}:{port}/{db}"


# ─────────────────────────────────────────────────────────────────────────────
#  SchedulerController
# ─────────────────────────────────────────────────────────────────────────────
class SchedulerController:
    """One instance per app session. Call start() once on app launch."""

    def __init__(self, state: AppState, app_queue: queue.Queue):
        global _REGISTRY_NEXT_KEY

        self._state    = state
        self._q        = app_queue
        self._scheduler: Optional[object] = None
        self._lock     = threading.Lock()

        # Register this instance so _run_scheduled_sync can find it
        self._registry_key  = _REGISTRY_NEXT_KEY
        _REGISTRY_NEXT_KEY += 1
        _REGISTRY[self._registry_key] = (state, app_queue)

    # ─────────────────────────────────────────────────────────────────────────
    #  Lifecycle
    # ─────────────────────────────────────────────────────────────────────────
    def start(self):
        """Start APScheduler. Call once on app launch."""
        if not HAS_APSCHEDULER:
            logger.warning("[Scheduler] APScheduler not installed — disabled")
            return

        try:
            jobstores = {}
            db_cfg = getattr(self._state, 'db_config', None)
            if db_cfg:
                jobstores["default"] = SQLAlchemyJobStore(
                    url=_build_url(db_cfg),
                    tablename="apscheduler_jobs",
                )

            self._scheduler = BackgroundScheduler(
                jobstores    = jobstores if jobstores else None,
                job_defaults = {
                    "coalesce":           True,
                    "max_instances":      1,
                    "misfire_grace_time": 300,
                },
                timezone="Asia/Kolkata",
            )

            self._scheduler.add_listener(
                self._on_job_event,
                EVENT_JOB_EXECUTED | EVENT_JOB_ERROR | EVENT_JOB_MISSED,
            )

            self._scheduler.start()
            logger.info("[Scheduler] APScheduler started")

            self._sync_all_jobs()

        except Exception as e:
            logger.error(f"[Scheduler] Failed to start: {e}")

    def shutdown(self):
        """Gracefully stop the scheduler and clean up registry."""
        if self._scheduler and self._scheduler.running:
            self._scheduler.shutdown(wait=False)
            logger.info("[Scheduler] Shutdown complete")
        _REGISTRY.pop(self._registry_key, None)

    # ─────────────────────────────────────────────────────────────────────────
    #  Job management
    # ─────────────────────────────────────────────────────────────────────────
    def add_or_update_job(self, company_name: str):
        """Add or reschedule a job for a company. Safe to call if job exists."""
        if not HAS_APSCHEDULER or not self._scheduler:
            return

        co = self._state.get_company(company_name)
        if not co or not co.schedule_enabled:
            self.remove_job(company_name)
            return

        job_id  = _slug(company_name)
        trigger = self._build_trigger(co)

        with self._lock:
            try:
                self._scheduler.add_job(
                    # ↓ module-level function — fully picklable
                    func             = _run_scheduled_sync,
                    trigger          = trigger,
                    id               = job_id,
                    name             = f"Sync: {company_name}",
                    kwargs           = {
                        "registry_key": self._registry_key,   # plain int
                        "company_name": company_name,          # plain str
                    },
                    replace_existing = True,
                )
                logger.info(
                    f"[Scheduler] Job added/updated: {job_id} "
                    f"({co.schedule_interval} × {co.schedule_value})"
                )
                self._post_schedule_update(company_name)
            except Exception as e:
                logger.error(f"[Scheduler] Failed to add job for {company_name}: {e}")

    def remove_job(self, company_name: str):
        if not HAS_APSCHEDULER or not self._scheduler:
            return
        job_id = _slug(company_name)
        try:
            if self._scheduler.get_job(job_id):
                self._scheduler.remove_job(job_id)
                logger.info(f"[Scheduler] Job removed: {job_id}")
                self._post_schedule_update(company_name)
        except Exception as e:
            logger.error(f"[Scheduler] Failed to remove job for {company_name}: {e}")

    def pause_job(self, company_name: str):
        if not self._scheduler:
            return
        try:
            self._scheduler.pause_job(_slug(company_name))
        except Exception:
            pass

    def resume_job(self, company_name: str):
        if not self._scheduler:
            return
        try:
            self._scheduler.resume_job(_slug(company_name))
        except Exception:
            pass

    def get_next_run(self, company_name: str) -> Optional[datetime]:
        if not self._scheduler:
            return None
        try:
            job = self._scheduler.get_job(_slug(company_name))
            return job.next_run_time if job else None
        except Exception:
            return None

    def get_all_jobs(self) -> list:
        if not self._scheduler:
            return []
        try:
            return self._scheduler.get_jobs()
        except Exception:
            return []

    def is_running(self) -> bool:
        return bool(self._scheduler and self._scheduler.running)

    # ─────────────────────────────────────────────────────────────────────────
    #  Trigger builder
    # ─────────────────────────────────────────────────────────────────────────
    @staticmethod
    def _build_trigger(co: CompanyState):
        if co.schedule_interval == "minutes":
            return IntervalTrigger(minutes=max(1, co.schedule_value))
        elif co.schedule_interval == "hourly":
            return IntervalTrigger(hours=max(1, co.schedule_value))
        elif co.schedule_interval == "daily":
            try:
                h, m = map(int, co.schedule_time.split(":"))
            except Exception:
                h, m = 9, 0
            return CronTrigger(hour=h, minute=m)
        return IntervalTrigger(hours=1)  # fallback

    # ─────────────────────────────────────────────────────────────────────────
    #  APScheduler event listener
    # ─────────────────────────────────────────────────────────────────────────
    def _on_job_event(self, event):
        job_id = getattr(event, "job_id", "")
        if not job_id.startswith("sync_"):
            return

        job = None
        try:
            job = self._scheduler.get_job(job_id)
        except Exception:
            pass

        company_name = (job.kwargs.get("company_name") if job else None) or job_id

        if hasattr(event, "exception") and event.exception:
            logger.error(f"[Scheduler] Job {job_id} failed: {event.exception}")
            self._q.put(("scheduler_job_error", company_name, str(event.exception)))
        else:
            self._post_schedule_update(company_name)

    def _post_schedule_update(self, company_name: str):
        self._q.put(("scheduler_updated", company_name))

    # ─────────────────────────────────────────────────────────────────────────
    #  Sync all enabled jobs on startup
    # ─────────────────────────────────────────────────────────────────────────
    def _sync_all_jobs(self):
        for name, co in self._state.companies.items():
            if co.schedule_enabled:
                self.add_or_update_job(name)