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

# ─────────────────────────────────────────────────────────────────────────────
#  Module-level SINGLETON  ←  the ONLY thing APScheduler pickles in kwargs
#
#  Problem with the old registry-key approach:
#    APScheduler persists job kwargs (including registry_key) to MySQL.
#    On next app launch the old key is loaded but the new session's _REGISTRY
#    hasn't been populated yet when the first job fires → "not ready" fallback.
#
#  Fix: store ONE module-level tuple that is always the LATEST live values.
#  _run_scheduled_sync() takes NO dynamic arguments — it always reads from here.
#  Jobs are stored with empty kwargs so no stale data survives a restart.
# ─────────────────────────────────────────────────────────────────────────────

_LIVE_STATE       = None   # AppState
_LIVE_GUI_QUEUE   = None   # queue.Queue  (to GUI)
_LIVE_SYNC_QUEUE  = None   # SyncQueueController | None


def _set_live(state, gui_queue, sync_queue_ctrl=None):
    """Called once by SchedulerController.__init__ — sets module-level live refs."""
    global _LIVE_STATE, _LIVE_GUI_QUEUE, _LIVE_SYNC_QUEUE
    _LIVE_STATE      = state
    _LIVE_GUI_QUEUE  = gui_queue
    _LIVE_SYNC_QUEUE = sync_queue_ctrl


def _update_sync_queue(sync_queue_ctrl):
    """Called by set_sync_queue() to register SyncQueueController after it starts."""
    global _LIVE_SYNC_QUEUE
    _LIVE_SYNC_QUEUE = sync_queue_ctrl


def _run_scheduled_sync(company_name: str):
    """
    Module-level function executed by APScheduler in a background thread.

    Uses module-level singleton (_LIVE_STATE / _LIVE_SYNC_QUEUE) so there
    are NO dynamic args baked into the persisted job — stale kwargs from
    previous sessions can never cause a "not ready" miss.

    Flow (Phase 2):
      → enqueue company into SyncQueueController (FIFO, Tally automation)
      → falls back to direct sync only if SyncQueueController unavailable
    """
    state      = _LIVE_STATE
    gui_queue  = _LIVE_GUI_QUEUE
    sync_queue = _LIVE_SYNC_QUEUE

    if state is None or gui_queue is None:
        logger.error(f"[Scheduler] Live state not set — job for '{company_name}' orphaned")
        return

    logger.info(f"[Scheduler] Triggered sync for: {company_name}")

    # ── Phase 2 path: enqueue into SyncQueueController ────────────────────
    if sync_queue is not None:
        sync_queue.enqueue(company_name)
        logger.info(f"[Scheduler] '{company_name}' enqueued in SyncQueueController ✓")
        return

    # ── Fallback: direct sync (SyncQueueController not available) ─────────
    logger.warning(
        f"[Scheduler] SyncQueueController not available — "
        f"running '{company_name}' directly (no Tally automation)"
    )
    co = state.companies.get(company_name)
    if co and co.syncing:
        logger.warning(f"[Scheduler] Skipping {company_name} — already syncing")
        return
    if state.sync_active:
        logger.warning(f"[Scheduler] Skipping {company_name} — manual sync active")
        return

    import time
    from gui.controllers.sync_controller import SyncController

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

    def __init__(self, state: AppState, app_queue: queue.Queue,
                 sync_queue_ctrl=None):
        """
        sync_queue_ctrl: SyncQueueController instance (Phase 2).
        Pass None if not yet available — set later via set_sync_queue().
        """
        self._state    = state
        self._q        = app_queue
        self._scheduler: Optional[object] = None
        self._lock     = threading.Lock()

        # Register module-level singleton so _run_scheduled_sync always finds
        # the latest live state regardless of persisted job kwargs
        _set_live(state, app_queue, sync_queue_ctrl)
        logger.info("[Scheduler] Live state registered in module singleton")

    def set_sync_queue(self, sync_queue_ctrl):
        """
        Register SyncQueueController after it starts.
        Updates the module singleton so the next scheduled job uses it.
        """
        _update_sync_queue(sync_queue_ctrl)
        logger.info("[Scheduler] SyncQueueController registered in singleton ✓")

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
                    # coalesce=True: if 10 runs were missed while app was closed,
                    # fire only ONE catch-up run — not all 10 at once.
                    "coalesce":           True,

                    # max_instances=1: never run same company twice at same time.
                    "max_instances":      1,

                    # misfire_grace_time: how old a missed job can be and still fire.
                    # Every-1-minute schedule + app closed for 2 hours = 120 missed.
                    # With coalesce=True only 1 fires, but we still want a short window.
                    # 60 seconds = only fire if missed by less than 1 minute.
                    # This prevents a stale catch-up sync firing hours later.
                    "misfire_grace_time": 60,
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
        """Gracefully stop the scheduler and clear module singleton."""
        if self._scheduler and self._scheduler.running:
            self._scheduler.shutdown(wait=False)
            logger.info("[Scheduler] Shutdown complete")
        _set_live(None, None, None)

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
                    func             = _run_scheduled_sync,
                    trigger          = trigger,
                    id               = job_id,
                    name             = f"Sync: {company_name}",
                    kwargs           = {
                        "company_name": company_name,   # only picklable arg needed
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
        """
        Load scheduler config from DB, then register APScheduler jobs.

        STARTUP SAFETY:
          When the app reopens after being closed, we DO NOT want jobs
          to fire immediately. Example:
            - App closed at 10:00, every-1-minute job was last run at 09:59
            - App reopens at 11:30
            - Without protection: job fires instantly at 11:30:00 → Tally
              gets hit before the app has finished loading
            - With startup_delay=30s: first run is 11:30:30 → app is ready

          We force every job's next_run_time to be:
            now + startup_delay (30 seconds)
          by passing jitter or by rescheduling with add_date_job.
          The simplest reliable way: call add_or_update_job() which always
          creates a fresh IntervalTrigger — APScheduler starts counting
          from NOW, so first fire is interval_seconds from now.
          For every-1-minute: first run = now + 60s. Safe.

        ORDER MATTERS:
          1. load_scheduler_config() reads DB → sets schedule_enabled etc.
          2. Then add_or_update_job() — otherwise schedule_enabled=False for all.
        """
        try:
            from gui.controllers.company_controller import CompanyController
            co_ctrl = CompanyController(self._state)
            co_ctrl.load_scheduler_config()
            logger.info("[Scheduler] Scheduler config loaded from DB ✓")
        except Exception as e:
            logger.error(f"[Scheduler] Failed to load scheduler config from DB: {e}")

        registered = 0
        for name, co in self._state.companies.items():
            if co.schedule_enabled:
                self._add_job_with_startup_delay(name, co)
                registered += 1

        logger.info(f"[Scheduler] Registered {registered} job(s). "
                    f"First run in ~{self.STARTUP_DELAY_SECONDS}s (startup delay).")

    # Delay before first job fires after app opens — gives app time to fully load
    STARTUP_DELAY_SECONDS = 30

    def _add_job_with_startup_delay(self, company_name: str, co):
        """
        Register job so it fires (interval) seconds AFTER the startup delay.
        This means:
          - Every-1-min job  → first fire at app_open + 30s + 60s  = 90s
          - Every-1-hour job → first fire at app_open + 30s + 3600s = ~1hr
        After the first fire, normal interval continues.

        Why not just add_or_update_job()?
          add_or_update_job uses IntervalTrigger which starts from NOW.
          For a 1-minute interval that is fine — first fire is 60s away.
          But for a daily CronTrigger, APScheduler calculates the next
          matching wall-clock time (e.g. 09:00 tomorrow) which is correct.
          So we only need special handling for interval-type jobs where
          the missed catch-up could fire too soon.
        """
        if not HAS_APSCHEDULER or not self._scheduler:
            return

        job_id  = _slug(company_name)
        trigger = self._build_trigger(co)

        # For CronTrigger (daily): APScheduler always fires at the correct
        # wall-clock time — no catch-up risk. Register normally.
        from apscheduler.triggers.cron import CronTrigger as _CT
        if isinstance(trigger, _CT):
            self.add_or_update_job(company_name)
            return

        # For IntervalTrigger (minutes/hourly): add a one-time startup delay.
        # We do this by setting start_date = now + startup_delay on the trigger.
        import datetime as _dt
        from apscheduler.triggers.interval import IntervalTrigger as _IT

        # Rebuild trigger with start_date so first fire is delayed
        if co.schedule_interval == "minutes":
            interval_seconds = max(1, co.schedule_value) * 60
        else:  # hourly
            interval_seconds = max(1, co.schedule_value) * 3600

        start_date = _dt.datetime.now() + _dt.timedelta(seconds=self.STARTUP_DELAY_SECONDS)
        delayed_trigger = _IT(
            seconds    = interval_seconds,
            start_date = start_date,
        )

        with self._lock:
            try:
                self._scheduler.add_job(
                    func             = _run_scheduled_sync,
                    trigger          = delayed_trigger,
                    id               = job_id,
                    name             = f"Sync: {company_name}",
                    kwargs           = {"company_name": company_name},
                    replace_existing = True,
                )
                logger.info(
                    f"[Scheduler] Job '{company_name}' registered — "
                    f"first run in {self.STARTUP_DELAY_SECONDS + interval_seconds}s"
                )
            except Exception as e:
                logger.error(f"[Scheduler] Failed to register job for '{company_name}': {e}")