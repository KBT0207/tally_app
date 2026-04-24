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

_LIVE_STATE       = None
_LIVE_GUI_QUEUE   = None
_LIVE_SYNC_QUEUE  = None

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

    if sync_queue is not None:
        sync_queue.enqueue(company_name)
        logger.info(f"[Scheduler] '{company_name}' enqueued in SyncQueueController ✓")
        return

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
    """Convert company name to a safe APScheduler job ID base."""
    return "sync_" + re.sub(r"[^a-zA-Z0-9_]", "_", name)

def _build_url(db_cfg: dict) -> str:
    """Build a pymysql connection URL from the db_config dict."""
    user = quote_plus(str(db_cfg.get("username", "root")))
    pw   = quote_plus(str(db_cfg.get("password", "")))
    host = db_cfg.get("host",     "localhost")
    port = int(db_cfg.get("port", 3306))
    db   = db_cfg.get("database", "tally_db")
    return f"mysql+pymysql://{user}:{pw}@{host}:{port}/{db}"

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

        _set_live(state, app_queue, sync_queue_ctrl)
        logger.info("[Scheduler] Live state registered in module singleton")

    def set_sync_queue(self, sync_queue_ctrl):
        """
        Register SyncQueueController after it starts.
        Updates the module singleton so the next scheduled job uses it.
        """
        _update_sync_queue(sync_queue_ctrl)
        logger.info("[Scheduler] SyncQueueController registered in singleton ✓")

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
                    "coalesce":      True,

                    "max_instances": 1,

                    "misfire_grace_time": 1,
                },
                timezone="Asia/Kolkata",
            )

            self._scheduler.add_listener(
                self._on_job_event,
                EVENT_JOB_EXECUTED | EVENT_JOB_ERROR | EVENT_JOB_MISSED,
            )

            self._scheduler.start()
            logger.info("[Scheduler] APScheduler started")

            try:
                self._scheduler.remove_all_jobs()
                logger.debug("[Scheduler] Cleared persisted jobs from store ✓")
            except Exception as _e:
                logger.warning(f"[Scheduler] Could not clear persisted jobs: {_e}")

            threading.Thread(target=self._sync_all_jobs, daemon=True).start()

        except Exception as e:
            logger.error(f"[Scheduler] Failed to start: {e}")

    def shutdown(self):
        """Gracefully stop the scheduler and clear module singleton."""
        if self._scheduler and self._scheduler.running:
            self._scheduler.shutdown(wait=False)
            logger.info("[Scheduler] Shutdown complete")
        _set_live(None, None, None)

    def add_or_update_job(self, company_name: str):
        """
        Add or reschedule job(s) for a company.
        For daily interval: registers one APScheduler job per configured time.
        For minutes/hourly: registers a single interval job (index _0).
        Safe to call if jobs already exist — replace_existing=True handles it.
        Also cleans up the old non-indexed job format for backward compat.
        """
        if not HAS_APSCHEDULER or not self._scheduler:
            return

        co = self._state.get_company(company_name)
        if not co or not co.schedule_enabled:
            self.remove_job(company_name)
            return

        triggers = self._build_trigger(co)
        slug     = _slug(company_name)

        with self._lock:
            try:
                try:
                    if self._scheduler.get_job(slug):
                        self._scheduler.remove_job(slug)
                        logger.info(f"[Scheduler] Removed legacy job ID '{slug}'")
                except Exception:
                    pass

                for i in range(len(triggers), 10):
                    old_id = f"{slug}_{i}"
                    try:
                        if self._scheduler.get_job(old_id):
                            self._scheduler.remove_job(old_id)
                    except Exception:
                        pass

                for i, trigger in enumerate(triggers):
                    job_id   = f"{slug}_{i}"
                    job_name = (
                        f"Sync: {company_name}"
                        if len(triggers) == 1
                        else f"Sync: {company_name} [{i + 1}/{len(triggers)}]"
                    )
                    self._scheduler.add_job(
                        func             = _run_scheduled_sync,
                        trigger          = trigger,
                        id               = job_id,
                        name             = job_name,
                        kwargs           = {"company_name": company_name},
                        replace_existing = True,
                    )

                logger.info(
                    f"[Scheduler] {len(triggers)} job(s) registered for '{company_name}' "
                    f"({co.schedule_interval} × {co.schedule_value})"
                )
                self._post_schedule_update(company_name)

            except Exception as e:
                logger.error(f"[Scheduler] Failed to add job for {company_name}: {e}")

    def remove_job(self, company_name: str):
        """Remove all APScheduler jobs for a company (indexed + legacy format)."""
        if not HAS_APSCHEDULER or not self._scheduler:
            return
        slug    = _slug(company_name)
        removed = False

        try:
            if self._scheduler.get_job(slug):
                self._scheduler.remove_job(slug)
                removed = True
        except Exception as e:
            logger.error(f"[Scheduler] Failed to remove legacy job for {company_name}: {e}")

        for i in range(10):
            job_id = f"{slug}_{i}"
            try:
                if self._scheduler.get_job(job_id):
                    self._scheduler.remove_job(job_id)
                    removed = True
            except Exception as e:
                logger.error(f"[Scheduler] Failed to remove job {job_id}: {e}")

        if removed:
            logger.info(f"[Scheduler] Job(s) removed for '{company_name}'")
            self._post_schedule_update(company_name)

    def pause_job(self, company_name: str):
        """Pause all jobs for a company."""
        if not self._scheduler:
            return
        slug = _slug(company_name)
        for job_id in [slug] + [f"{slug}_{i}" for i in range(10)]:
            try:
                if self._scheduler.get_job(job_id):
                    self._scheduler.pause_job(job_id)
            except Exception:
                pass

    def resume_job(self, company_name: str):
        """Resume all jobs for a company."""
        if not self._scheduler:
            return
        slug = _slug(company_name)
        for job_id in [slug] + [f"{slug}_{i}" for i in range(10)]:
            try:
                if self._scheduler.get_job(job_id):
                    self._scheduler.resume_job(job_id)
            except Exception:
                pass

    def pause_all(self):
        """
        Pause ALL scheduled jobs at once.
        Called from tray menu → app.py._toggle_pause() → here.
        Uses APScheduler's scheduler-level pause (not per-job).
        All jobs stay registered — they just won't fire until resume_all().
        """
        if not self._scheduler:
            logger.warning("[Scheduler] pause_all() called but scheduler not running")
            return
        try:
            self._scheduler.pause()
            logger.info("[Scheduler] All jobs paused ✓")
        except Exception as e:
            logger.error(f"[Scheduler] pause_all() failed: {e}")

    def resume_all(self):
        """
        Resume ALL scheduled jobs at once.
        Called from tray menu → app.py._toggle_pause() → here.
        Jobs fire again on their normal schedule from this point forward.
        """
        if not self._scheduler:
            logger.warning("[Scheduler] resume_all() called but scheduler not running")
            return
        try:
            self._scheduler.resume()
            logger.info("[Scheduler] All jobs resumed ✓")
        except Exception as e:
            logger.error(f"[Scheduler] resume_all() failed: {e}")

    def get_next_run(self, company_name: str) -> Optional[datetime]:
        """
        Return the earliest next_run_time across all APScheduler jobs
        for this company (supports multiple daily times).
        """
        if not self._scheduler:
            return None
        try:
            slug      = _slug(company_name)
            next_runs = []

            job = self._scheduler.get_job(slug)
            if job and job.next_run_time:
                next_runs.append(job.next_run_time)

            for i in range(10):
                job = self._scheduler.get_job(f"{slug}_{i}")
                if job and job.next_run_time:
                    next_runs.append(job.next_run_time)

            return min(next_runs) if next_runs else None
        except Exception:
            return None

    def get_all_next_runs(self) -> dict:
        """
        Return {company_name: next_run_time} for every registered job in ONE
        get_jobs() call instead of 11 get_job() calls per company.

        Replaces the old pattern of calling get_next_run() per-company from the
        GUI thread which did 23 × 11 = 253 individual MySQL round-trips each
        time the scheduler page opened, freezing the app for 30s+.
        """
        if not self._scheduler:
            return {}
        try:
            result: dict = {}
            for job in self._scheduler.get_jobs():
                if not job.id.startswith("sync_"):
                    continue
                company_name = (job.kwargs or {}).get("company_name")
                if not company_name or not job.next_run_time:
                    continue
                existing = result.get(company_name)
                if existing is None or job.next_run_time < existing:
                    result[company_name] = job.next_run_time
            return result
        except Exception:
            return {}

    def get_all_jobs(self) -> list:
        if not self._scheduler:
            return []
        try:
            return self._scheduler.get_jobs()
        except Exception:
            return []

    def is_running(self) -> bool:
        return bool(self._scheduler and self._scheduler.running)

    @staticmethod
    def _build_trigger(co: CompanyState) -> list:
        """
        Build and return a LIST of APScheduler triggers for a company.

        - minutes / hourly  → always returns [single IntervalTrigger]
        - daily             → returns one CronTrigger per comma-separated time
                              e.g. schedule_time="01:00,14:00" → 2 CronTriggers
        - fallback          → [IntervalTrigger(hours=1)]

        Callers must loop over the returned list and register one job per trigger.
        """
        if co.schedule_interval == "minutes":
            return [IntervalTrigger(minutes=max(1, co.schedule_value))]

        elif co.schedule_interval == "hourly":
            return [IntervalTrigger(hours=max(1, co.schedule_value))]

        elif co.schedule_interval == "daily":
            triggers = []
            raw_times = [t.strip() for t in co.schedule_time.split(",") if t.strip()]
            for t in raw_times:
                try:
                    h, m = map(int, t.split(":"))
                except Exception:
                    h, m = 9, 0
                triggers.append(CronTrigger(hour=h, minute=m))
            return triggers if triggers else [CronTrigger(hour=9, minute=0)]

        return [IntervalTrigger(hours=1)]

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

    STAGGER_SECONDS = 10

    def _sync_all_jobs(self):
        """
        Load scheduler config from DB, then register APScheduler jobs.

        AUTO-STAGGER:
          Companies that share the same interval key are spread out by
          STAGGER_SECONDS (10s) each so they never all fire simultaneously.

          Example — 6 companies, all "every 1 hour":
            CompanyA  → start_date = base + 0s   → fires at 10:00:00
            CompanyB  → start_date = base + 10s  → fires at 10:00:10
            CompanyC  → start_date = base + 20s  → fires at 10:00:20
            CompanyD  → start_date = base + 30s  → fires at 10:00:30
            CompanyE  → start_date = base + 40s  → fires at 10:00:40
            CompanyF  → start_date = base + 50s  → fires at 10:00:50

          After the first fire, each company repeats on its own independent
          interval — so they stay staggered every hour automatically.

          Companies with DIFFERENT intervals are NOT staggered against each
          other — only companies sharing the exact same interval key.

          Daily (CronTrigger) jobs are not staggered — APScheduler fires
          them at the configured wall-clock time and they don't pile up.
        """
        try:
            from gui.controllers.company_controller import CompanyController
            co_ctrl = CompanyController(self._state)
            co_ctrl.load_scheduler_config()
            logger.info("[Scheduler] Scheduler config loaded from DB ✓")
        except Exception as e:
            logger.error(f"[Scheduler] Failed to load scheduler config from DB: {e}")

        try:
            self._scheduler.pause()
        except Exception:
            pass

        import collections
        interval_groups: dict = collections.defaultdict(list)

        enabled_companies = [
            (name, co)
            for name, co in self._state.companies.items()
            if co.schedule_enabled
        ]

        for name, co in enabled_companies:
            if co.schedule_interval == "daily":
                self.add_or_update_job(name)
            else:
                key = (co.schedule_interval, co.schedule_value)
                interval_groups[key].append((name, co))

        total_registered = 0
        for interval_key, group in interval_groups.items():
            group_sorted = sorted(group, key=lambda x: x[0].lower())

            for position, (name, co) in enumerate(group_sorted):
                stagger_offset = position * self.STAGGER_SECONDS
                self._add_job_with_startup_delay(name, co, stagger_offset)
                total_registered += 1

                if len(group_sorted) > 1:
                    logger.info(
                        f"[Scheduler] '{name}' stagger offset: +{stagger_offset}s "
                        f"(position {position + 1}/{len(group_sorted)} "
                        f"in group {interval_key})"
                    )

        daily_count = sum(
            1 for _, co in enabled_companies
            if co.schedule_interval == "daily"
        )
        total_registered += daily_count

        logger.info(
            f"[Scheduler] Registered jobs for {total_registered} company/companies "
            f"({len(interval_groups)} interval group(s), "
            f"{daily_count} daily). "
            f"Stagger: {self.STAGGER_SECONDS}s between companies "
            f"sharing the same interval."
        )

        try:
            self._scheduler.resume()
        except Exception:
            pass

    STARTUP_DELAY_SECONDS = 30

    def _add_job_with_startup_delay(self, company_name: str, co,
                                     stagger_offset: int = 0):
        """
        Register a single interval-based job (minutes/hourly) with a smart
        start_date so next_run_time is correct after restart.

        For daily companies, add_or_update_job() is called directly from
        _sync_all_jobs() — this method is only used for interval jobs.

        stagger_offset: extra seconds added on top of startup delay so
          companies sharing the same interval fire at different times.

        Respects last_sync_time:
          1. If last_sync_time exists → compute ideal_next = last_sync + interval
          2. If ideal_next is in future → use it + stagger_offset
          3. If ideal_next is in past (missed) → use now + startup_delay + stagger_offset
             MissedSyncChecker handles the one catch-up run independently.
        """
        if not HAS_APSCHEDULER or not self._scheduler:
            return

        import datetime as _dt
        from apscheduler.triggers.cron     import CronTrigger as _CT

        slug    = _slug(company_name)
        now     = _dt.datetime.now()
        triggers = self._build_trigger(co)

        if triggers and isinstance(triggers[0], _CT):
            self.add_or_update_job(company_name)
            return

        if co.schedule_interval == "minutes":
            interval_seconds = max(1, co.schedule_value) * 60
        else:
            interval_seconds = max(1, co.schedule_value) * 3600

        interval_delta = _dt.timedelta(seconds=interval_seconds)

        last_sync  = getattr(co, 'last_sync_time', None)
        ideal_next = None

        if last_sync:
            if hasattr(last_sync, 'tzinfo') and last_sync.tzinfo is not None:
                last_sync = last_sync.replace(tzinfo=None)
            ideal_next = last_sync + interval_delta

        min_start = now + _dt.timedelta(
            seconds = self.STARTUP_DELAY_SECONDS + stagger_offset
        )

        if ideal_next and ideal_next > min_start:
            start_date = ideal_next + _dt.timedelta(seconds=stagger_offset)
            logger.info(
                f"[Scheduler] '{company_name}' — next run preserved "
                f"(+{stagger_offset}s stagger): "
                f"{start_date.strftime('%d %b %Y %H:%M:%S')}"
            )
        else:
            start_date = min_start
            if ideal_next:
                logger.info(
                    f"[Scheduler] '{company_name}' — missed, "
                    f"next run: {start_date.strftime('%H:%M:%S')} "
                    f"(+{stagger_offset}s stagger)"
                )
            else:
                logger.info(
                    f"[Scheduler] '{company_name}' — no last sync, "
                    f"first run: {start_date.strftime('%H:%M:%S')} "
                    f"(+{stagger_offset}s stagger)"
                )

        from apscheduler.triggers.interval import IntervalTrigger as _IT
        delayed_trigger = _IT(
            seconds    = interval_seconds,
            start_date = start_date,
        )

        job_id = f"{slug}_0"

        with self._lock:
            try:
                try:
                    if self._scheduler.get_job(slug):
                        self._scheduler.remove_job(slug)
                except Exception:
                    pass

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
                    f"start_date: {start_date.strftime('%d %b %Y %H:%M:%S')}"
                )
            except Exception as e:
                logger.error(
                    f"[Scheduler] Failed to register job for '{company_name}': {e}"
                )
