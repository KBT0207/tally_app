"""
gui/controllers/sync_queue_controller.py
==========================================
FIFO queue + single worker thread for sequential company syncing.

Design:
  - APScheduler jobs (and manual triggers) call enqueue(company_name)
  - A single daemon worker thread picks one company at a time
  - TallyLauncher.prepare() runs first → then SyncController
  - One company fully completes before the next starts
  - Failures are logged and the queue continues — never blocked

Queue message format emitted to GUI queue:
  ("sync_queue_started",  company_name)
  ("sync_queue_done",     company_name, success: bool)
  ("sync_queue_log",      company_name, message, level)
  ("sync_queue_progress", company_name, pct, label)
"""

import queue
import threading
import time
from datetime import datetime
from typing import Optional

from gui.state   import AppState, CompanyStatus
from logging_config import logger


class SyncQueueController:
    """
    Single instance per app session.
    Call start() once on app launch.
    Call enqueue(company_name) from scheduler jobs or manual triggers.
    Call shutdown() on app close.
    """

    def __init__(self, state: AppState, app_queue: queue.Queue):
        self._state     = state
        self._app_q     = app_queue           # GUI queue (polled by app.py)
        self._queue     = queue.Queue()       # FIFO company name queue
        self._worker_t: Optional[threading.Thread] = None
        self._running   = False
        self._lock      = threading.Lock()

        # Currently processing — readable by UI
        self.current_company: Optional[str] = None
        # Snapshot of queue contents for UI display
        self.queued_companies: list[str] = []

    # ─────────────────────────────────────────────────────────────────────────
    #  Lifecycle
    # ─────────────────────────────────────────────────────────────────────────
    def start(self):
        """Start the worker thread. Call once at app launch."""
        with self._lock:
            if self._running:
                logger.warning("[SyncQueue] Already running — ignoring start()")
                return
            self._running = True

        self._worker_t = threading.Thread(
            target  = self._worker_loop,
            daemon  = True,
            name    = "SyncQueueWorker",
        )
        self._worker_t.start()
        logger.info("[SyncQueue] Worker thread started")

    def shutdown(self):
        """Signal worker to stop. Call on app close."""
        with self._lock:
            self._running = False
        # Unblock the worker if it's waiting on an empty queue
        self._queue.put(None)
        logger.info("[SyncQueue] Shutdown signal sent")

    # ─────────────────────────────────────────────────────────────────────────
    #  Public API — called by scheduler jobs
    # ─────────────────────────────────────────────────────────────────────────
    def enqueue(self, company_name: str):
        """
        Add a company to the sync queue. Non-blocking.
        Called by APScheduler job functions and manual sync triggers.
        """
        # Don't enqueue if already in the queue or currently processing
        if company_name == self.current_company:
            logger.warning(f"[SyncQueue] '{company_name}' is already being synced — skipping")
            return

        if company_name in self.queued_companies:
            logger.warning(f"[SyncQueue] '{company_name}' is already queued — skipping duplicate")
            return

        self._queue.put(company_name)
        with self._lock:
            self.queued_companies.append(company_name)

        logger.info(f"[SyncQueue] Enqueued: '{company_name}' | Queue size: {self._queue.qsize()}")
        self._post("sync_queue_log", company_name, f"⏳ Queued for sync", "INFO")

        # Notify GUI that queue changed
        self._app_q.put(("queue_updated", None))

    @property
    def is_idle(self) -> bool:
        """True when nothing is running and queue is empty."""
        return self.current_company is None and self._queue.empty()

    @property
    def queue_size(self) -> int:
        return self._queue.qsize()

    # ─────────────────────────────────────────────────────────────────────────
    #  Worker loop — runs in daemon thread
    # ─────────────────────────────────────────────────────────────────────────
    def _worker_loop(self):
        """
        Continuously picks one company from the queue and processes it.
        Blocks on queue.get() when queue is empty — no CPU waste.
        """
        logger.info("[SyncQueue] Worker loop started — waiting for jobs...")

        while True:
            # Block until a company name arrives (or None for shutdown)
            company_name = self._queue.get()

            # Shutdown signal
            if company_name is None:
                logger.info("[SyncQueue] Worker loop shutting down")
                break

            # Check if still running (shutdown may have been called)
            with self._lock:
                if not self._running:
                    self._queue.task_done()
                    break
                self.current_company = company_name
                # Remove from queued list now that we're processing
                if company_name in self.queued_companies:
                    self.queued_companies.remove(company_name)

            logger.info(f"[SyncQueue] ─── Processing: '{company_name}' ───")
            self._app_q.put(("queue_updated", None))

            success = False
            try:
                success = self._process_company(company_name)
            except Exception as e:
                logger.exception(f"[SyncQueue] Unhandled error for '{company_name}': {e}")
                self._post("sync_queue_log", company_name,
                           f"✗ Unexpected error: {e}", "ERROR")
                self._state.set_company_status(company_name, CompanyStatus.SYNC_ERROR)
            finally:
                with self._lock:
                    self.current_company = None
                self._queue.task_done()
                self._app_q.put(("queue_updated", None))
                self._app_q.put(("sync_queue_done", company_name, success))
                logger.info(
                    f"[SyncQueue] Finished: '{company_name}' "
                    f"({'OK' if success else 'FAILED'}) "
                    f"| Remaining: {self._queue.qsize()}"
                )

    # ─────────────────────────────────────────────────────────────────────────
    #  Process one company — Tally automation + sync
    # ─────────────────────────────────────────────────────────────────────────
    def _process_company(self, company_name: str) -> bool:
        """
        Full pipeline for one company:
          1. TallyLauncher.prepare() — open Tally + correct company
          2. SyncController — run XML sync
        Returns True on success, False on any failure.
        """
        self._post("sync_queue_started", company_name)
        self._state.set_company_status(company_name, CompanyStatus.SYNCING)

        # ── PHASE A: Tally Automation ─────────────────────────────────────
        self._post("sync_queue_log", company_name,
                   "🤖 Starting Tally automation...", "INFO")

        try:
            from services.tally_launcher import TallyLauncher
            launcher = TallyLauncher(self._state)
            ok, msg  = launcher.prepare(company_name)

            if not ok:
                self._post("sync_queue_log", company_name,
                           f"✗ Tally automation failed: {msg}", "ERROR")
                self._state.set_company_status(company_name, CompanyStatus.SYNC_ERROR)
                return False

            self._post("sync_queue_log", company_name,
                       "✓ Tally ready — starting sync...", "SUCCESS")

        except Exception as e:
            self._post("sync_queue_log", company_name,
                       f"✗ TallyLauncher error: {e}", "ERROR")
            self._state.set_company_status(company_name, CompanyStatus.SYNC_ERROR)
            return False

        # ── PHASE B: XML Sync ─────────────────────────────────────────────
        return self._run_sync(company_name)

    def _run_sync(self, company_name: str) -> bool:
        """
        Run SyncController for one company and wait for completion.
        Blocks until sync is fully done.

        Auto-detects sync mode:
          - is_initial_done = False  →  SNAPSHOT from company's starting_from date
          - is_initial_done = True   →  INCREMENTAL (alter_id CDC)

        Returns True on success, False on failure.
        """
        from gui.controllers.sync_controller import SyncController
        from gui.state import SyncMode

        job_q   = queue.Queue()
        success = False

        co_state = self._state.get_company(company_name)

        # ── Determine sync mode ───────────────────────────────────────────
        if co_state and not co_state.is_initial_done:
            # First ever sync — must do full snapshot
            sync_mode = SyncMode.SNAPSHOT
            from_date = (co_state.starting_from or co_state.books_from or
                         datetime.now().strftime("%Y0401"))
            self._post(
                "sync_queue_log", company_name,
                f"⚠ No initial snapshot yet — running full snapshot from {from_date}",
                "WARNING",
            )
        else:
            # Subsequent syncs — incremental via alter_id
            sync_mode = SyncMode.INCREMENTAL
            from_date = None
            self._post("sync_queue_log", company_name, "→ Running incremental sync", "INFO")

        controller = SyncController(
            state      = self._state,
            out_queue  = job_q,
            companies  = [company_name],
            sync_mode  = sync_mode,
            from_date  = from_date,
            to_date    = datetime.now().strftime("%Y%m%d"),
            vouchers   = self._state.voucher_selection,
            sequential = True,
        )
        controller.start()

        # Drain job_q — forward to GUI queue — wait for all_done
        timeout_no_msg = 300   # 5 min max silence before giving up
        last_msg_time  = time.time()

        while True:
            try:
                msg = job_q.get(timeout=1.0)
                last_msg_time = time.time()

                event = msg[0]

                if event == "log":
                    _, co_name, text, level = msg
                    self._post("sync_queue_log", co_name, text, level)
                    # Also forward to logs page
                    self._app_q.put(("sync_log", text))

                elif event == "progress":
                    _, co_name, pct, label = msg
                    self._post("sync_queue_progress", co_name, pct, label)
                    self._state.set_company_progress(co_name, pct, label)

                elif event == "status":
                    _, co_name, status = msg
                    self._state.set_company_status(co_name, status)

                elif event == "done":
                    _, co_name, ok = msg
                    success = ok

                elif event == "all_done":
                    break

            except queue.Empty:
                # Check for silence timeout
                if time.time() - last_msg_time > timeout_no_msg:
                    logger.error(
                        f"[SyncQueue] Sync for '{company_name}' timed out "
                        f"(no message for {timeout_no_msg}s)"
                    )
                    success = False
                    break
                # Also check if state.sync_active went False (controller finished)
                if not self._state.sync_active:
                    break

        return success


    # ─────────────────────────────────────────────────────────────────────────
    #  Helper
    # ─────────────────────────────────────────────────────────────────────────
    def _post(self, *args):
        """Put a message on the GUI app queue."""
        self._app_q.put(args)