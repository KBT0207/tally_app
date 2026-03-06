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

─────────────────────────────────────────────────────────────────────────────
FIXES APPLIED
─────────────────────────────────────────────────────────────────────────────
🔴 FIX 1 — enqueue() race condition fixed
     The duplicate check (current_company + queued_companies) is now
     fully inside a single self._lock block. Previously the check and
     the append were in separate lock sections — a second thread could
     sneak in between them and enqueue the same company twice.

🔴 FIX 2 — Removed dangerous sync_active fallback in _run_sync()
     The old code broke out of the drain loop if self._state.sync_active
     was False — even if the sync was still running. This caused the next
     company to start before the current one truly finished.
     Now we only trust two exit conditions: "all_done" event OR timeout.

🟡 FIX 3 — Retry logic added (2 retries, 60s delay)
     If a company sync fails (Tally offline, network blip etc.) it is
     automatically re-enqueued after 60 seconds, up to 2 times.
     Retry count resets to 0 after all retries are exhausted so the
     next scheduled run starts fresh.
─────────────────────────────────────────────────────────────────────────────
"""

import queue
import threading
import time
from datetime import datetime
from typing import Optional

from gui.state   import AppState, CompanyStatus
from logging_config import logger


# ─────────────────────────────────────────────────────────────────────────────
#  Constants
# ─────────────────────────────────────────────────────────────────────────────
MAX_RETRIES        = 2     # How many times to retry a failed company
RETRY_DELAY_SEC    = 60    # Seconds to wait before re-enqueuing a failed company
TIMEOUT_NO_MSG_SEC = 300   # 5 min max silence in _run_sync before giving up


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

        # 🟡 FIX 3 — Retry counter per company  { company_name: retry_count }
        self._retry_counts: dict[str, int] = {}

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
    #  Public API — called by scheduler jobs and manual triggers
    # ─────────────────────────────────────────────────────────────────────────
    def enqueue(self, company_name: str):
        """
        Add a company to the sync queue. Non-blocking.
        Called by APScheduler job functions and manual sync triggers.

        🔴 FIX 1: The entire duplicate check + append is now inside ONE
        lock block. This eliminates the race condition where two threads
        could both pass the check before either had appended to the list.
        """
        with self._lock:
            # ── Check 1: already being synced right now ───────────────────
            if company_name == self.current_company:
                logger.warning(
                    f"[SyncQueue] '{company_name}' is already being synced — skipping"
                )
                return

            # ── Check 2: already sitting in the queue ─────────────────────
            if company_name in self.queued_companies:
                logger.warning(
                    f"[SyncQueue] '{company_name}' is already queued — skipping duplicate"
                )
                return

            # ── Safe to enqueue ───────────────────────────────────────────
            self.queued_companies.append(company_name)

        # Put on queue AFTER releasing lock (queue.put is thread-safe itself)
        self._queue.put(company_name)

        logger.info(f"[SyncQueue] Enqueued: '{company_name}' | Queue size: {self._queue.qsize()}")
        self._post("sync_queue_log", company_name, "⏳ Queued for sync", "INFO")
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
        One company fully completes before the next one starts.
        """
        logger.info("[SyncQueue] Worker loop started — waiting for jobs...")

        # Track each round: when it started and which companies ran in it
        # Used to detect overrun (total round time > company's interval)
        self._round_start:    float     = 0.0
        self._round_companies: list[str] = []

        while True:
            # Block until a company name arrives (or None for shutdown)
            company_name = self._queue.get()

            # ── Shutdown signal ───────────────────────────────────────────
            if company_name is None:
                logger.info("[SyncQueue] Worker loop shutting down")
                self._queue.task_done()
                break

            # ── Check if shutdown was called while we were waiting ────────
            with self._lock:
                if not self._running:
                    self._queue.task_done()
                    break
                self.current_company = company_name
                if company_name in self.queued_companies:
                    self.queued_companies.remove(company_name)

            # ── Start round tracking if queue was empty before this ───────
            if not self._round_companies:
                self._round_start = time.time()
                self._round_companies = []

            logger.info(f"[SyncQueue] ─── Processing: '{company_name}' ───")
            self._app_q.put(("queue_updated", None))

            company_start = time.time()
            success       = False
            try:
                success = self._process_company(company_name)
            except Exception as e:
                logger.exception(f"[SyncQueue] Unhandled error for '{company_name}': {e}")
                self._post("sync_queue_log", company_name, f"✗ Unexpected error: {e}", "ERROR")
                self._state.set_company_status(company_name, CompanyStatus.SYNC_ERROR)
            finally:
                company_elapsed = time.time() - company_start
                with self._lock:
                    self.current_company = None
                self._queue.task_done()
                self._app_q.put(("queue_updated", None))
                self._app_q.put(("sync_queue_done", company_name, success))
                logger.info(
                    f"[SyncQueue] Finished: '{company_name}' "
                    f"({'OK' if success else 'FAILED'}) "
                    f"in {company_elapsed:.0f}s "
                    f"| Remaining: {self._queue.qsize()}"
                )

            # ── FIX 7: Reset retry count on success ───────────────────────
            # Previously retry_count was only reset after MAX_RETRIES exhausted.
            # This meant a company that failed once (retry_count=1) and then
            # succeeded would only have 1 retry left on its next failure
            # instead of the full MAX_RETRIES. Now we reset on every success.
            if success:
                with self._lock:
                    if self._retry_counts.get(company_name, 0) > 0:
                        self._retry_counts[company_name] = 0
                        logger.debug(
                            f"[SyncQueue] '{company_name}' succeeded — "
                            f"retry counter reset to 0"
                        )

            # ── Retry on failure ──────────────────────────────────────────
            if not success:
                self._handle_retry(company_name)

            # ── Track company in this round ───────────────────────────────
            self._round_companies.append(company_name)

            # ── FIX 2: Check for round overrun when queue becomes empty ───
            # When queue is empty this round is complete — check if total
            # time exceeded the shortest interval of any company in the round.
            if self._queue.empty() and self._round_companies:
                self._check_round_overrun()
                self._round_companies = []
                self._round_start     = 0.0

    # ─────────────────────────────────────────────────────────────────────────
    #  FIX 2 — Round overrun detection
    # ─────────────────────────────────────────────────────────────────────────
    def _check_round_overrun(self):
        """
        Called after every round completes (queue goes empty).

        Checks: did the total round time exceed the shortest scheduled
        interval of any company that ran in this round?

        If yes → logs a WARNING and posts an overrun event to the GUI
        so app.py can show an amber banner to the user.

        Example:
          6 companies, every 1 hour
          Round took 75 minutes total
          → WARNING: round exceeded 1hr interval by 15 min
          → Suggests: increase interval to at least 80 min
        """
        if not self._round_start or not self._round_companies:
            return

        round_elapsed = time.time() - self._round_start
        round_elapsed_min = round_elapsed / 60

        # Find shortest interval (in seconds) among companies in this round
        shortest_interval_sec = None
        shortest_company      = None

        for name in self._round_companies:
            co = self._state.get_company(name)
            if not co or not getattr(co, 'schedule_enabled', False):
                continue

            interval = getattr(co, 'schedule_interval', 'hourly')
            value    = max(1, int(getattr(co, 'schedule_value', 1)))

            if interval == "minutes":
                interval_sec = value * 60
            elif interval == "hourly":
                interval_sec = value * 3600
            else:
                continue  # daily — no overrun concern

            if shortest_interval_sec is None or interval_sec < shortest_interval_sec:
                shortest_interval_sec = interval_sec
                shortest_company      = name

        if shortest_interval_sec is None:
            return

        shortest_interval_min = shortest_interval_sec / 60

        if round_elapsed > shortest_interval_sec:
            overrun_min     = round_elapsed_min - shortest_interval_min
            suggested_min   = int(round_elapsed_min * 1.2)  # 20% buffer

            logger.warning(
                f"[SyncQueue] ⚠ OVERRUN DETECTED — "
                f"Round took {round_elapsed_min:.1f}min, "
                f"shortest interval is {shortest_interval_min:.0f}min "
                f"(overrun by {overrun_min:.1f}min). "
                f"Suggest increasing interval to at least {suggested_min}min."
            )

            # Post to GUI so app.py can show banner
            self._app_q.put((
                "sync_overrun_detected",
                self._round_companies.copy(),
                round_elapsed_min,
                shortest_interval_min,
                suggested_min,
            ))
        else:
            logger.info(
                f"[SyncQueue] Round complete — "
                f"{len(self._round_companies)} companies in "
                f"{round_elapsed_min:.1f}min "
                f"(interval: {shortest_interval_min:.0f}min) ✓"
            )

    # ─────────────────────────────────────────────────────────────────────────
    #  Retry handler
    # ─────────────────────────────────────────────────────────────────────────
    def _handle_retry(self, company_name: str):
        """
        If a company sync fails, re-enqueue it after RETRY_DELAY_SEC seconds.
        Maximum MAX_RETRIES attempts. After that, reset counter and give up
        until the next scheduled run.
        """
        with self._lock:
            retries = self._retry_counts.get(company_name, 0)

        if retries < MAX_RETRIES:
            next_attempt = retries + 1
            with self._lock:
                self._retry_counts[company_name] = next_attempt

            logger.warning(
                f"[SyncQueue] '{company_name}' failed — "
                f"retry {next_attempt}/{MAX_RETRIES} in {RETRY_DELAY_SEC}s"
            )
            self._post(
                "sync_queue_log", company_name,
                f"⚠ Sync failed — retrying in {RETRY_DELAY_SEC}s "
                f"(attempt {next_attempt}/{MAX_RETRIES})",
                "WARNING",
            )

            # Re-enqueue after delay in a background thread
            # so the worker loop is not blocked during the wait
            def _delayed_retry(name=company_name):
                time.sleep(RETRY_DELAY_SEC)
                logger.info(f"[SyncQueue] Re-enqueuing '{name}' after retry delay")
                self.enqueue(name)

            threading.Thread(
                target  = _delayed_retry,
                daemon  = True,
                name    = f"RetryDelay-{company_name}",
            ).start()

        else:
            # All retries exhausted — reset counter, log final failure
            with self._lock:
                self._retry_counts[company_name] = 0

            logger.error(
                f"[SyncQueue] '{company_name}' failed after {MAX_RETRIES} retries — giving up"
            )
            self._post(
                "sync_queue_log", company_name,
                f"✗ Sync failed after {MAX_RETRIES} retries — will retry at next scheduled time",
                "ERROR",
            )

    # ─────────────────────────────────────────────────────────────────────────
    #  Process one company — Tally automation + sync
    # ─────────────────────────────────────────────────────────────────────────
    def _process_company(self, company_name: str) -> bool:
        """
        Full pipeline for one company:
          1. TallyLauncher.prepare() — open Tally + switch to correct company
          2. SyncController        — run XML sync
        Returns True on success, False on any failure.
        """
        self._post("sync_queue_started", company_name)
        self._state.set_company_status(company_name, CompanyStatus.SYNCING)

        # ── PHASE A: Tally Automation ─────────────────────────────────────
        self._post("sync_queue_log", company_name, "🤖 Starting Tally automation...", "INFO")

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
        Run SyncController for one company and block until fully complete.

        Auto-detects sync mode:
          - is_initial_done = False  →  SNAPSHOT from company's starting_from date
          - is_initial_done = True   →  INCREMENTAL (alter_id CDC)

        🔴 FIX 2: Removed the 'if not self._state.sync_active: break' fallback.
        That check was dangerous — sync_active could be False for unrelated
        reasons and would cause this method to return early (success=False)
        while the sync was still actually running, letting the next company
        start too soon.

        Now we only exit via two safe conditions:
          1. "all_done" event received from SyncController  ← normal exit
          2. TIMEOUT_NO_MSG_SEC of silence                  ← safety net
        """
        from gui.controllers.sync_controller import SyncController
        from gui.state import SyncMode

        job_q   = queue.Queue()
        success = False

        co_state = self._state.get_company(company_name)

        # ── Determine sync mode ───────────────────────────────────────────
        if co_state and not co_state.is_initial_done:
            sync_mode = SyncMode.SNAPSHOT
            from_date = (
                co_state.starting_from
                or co_state.books_from
                or datetime.now().strftime("%Y0401")
            )
            self._post(
                "sync_queue_log", company_name,
                f"⚠ No initial snapshot yet — running full snapshot from {from_date}",
                "WARNING",
            )
        else:
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

        # ── Drain job_q — forward messages to GUI — wait for all_done ────
        last_msg_time = time.time()

        while True:
            try:
                msg = job_q.get(timeout=1.0)
                last_msg_time = time.time()

                event = msg[0]

                if event == "log":
                    _, co_name, text, level = msg
                    self._post("sync_queue_log", co_name, text, level)
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
                    # ✅ Normal exit — SyncController confirmed it is finished
                    break

            except queue.Empty:
                # 🔴 FIX 2 — Only timeout is the fallback, NOT sync_active
                elapsed = time.time() - last_msg_time
                if elapsed > TIMEOUT_NO_MSG_SEC:
                    logger.error(
                        f"[SyncQueue] Sync for '{company_name}' timed out "
                        f"(no message for {TIMEOUT_NO_MSG_SEC}s) — forcing exit"
                    )
                    success = False
                    break
                # Otherwise keep waiting — do NOT check sync_active here

        return success

    # ─────────────────────────────────────────────────────────────────────────
    #  Helper
    # ─────────────────────────────────────────────────────────────────────────
    def _post(self, *args):
        """Put a message on the GUI app queue."""
        self._app_q.put(args)