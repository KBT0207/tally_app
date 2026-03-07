"""
gui/controllers/sync_queue_controller.py
==========================================
FIFO queue + single worker thread for sequential company syncing.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
THREE CORE QUEUE RULES  (Industry Standard)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

RULE 1 — NO DUPLICATES EVER
    A company can only exist in ONE of these three states at a time:
      • Currently running   (current_company)
      • Waiting in queue    (queued_companies)
      • Already ran this round (_round_companies)
    If a scheduler fires and the company is in ANY of these → SKIP.
    This prevents double-processing even with mixed intervals
    (e.g. Company A every 10min, Company B every 15min).

RULE 2 — ONE ROUND AT A TIME  (Round Gate)
    A round starts when the first company enters an empty queue.
    A round ends when the queue is completely empty.
    No new round can start until the current round finishes.
    Late-arriving companies (different interval) JOIN the current round —
    they do not start a new one.
    _round_active  = True  while any company is queued or running
    _round_active  = False only when queue goes fully empty

RULE 3 — QUEUE NEVER BLOCKS
    If one company crashes, errors, or times out → skip it, continue.
    A single bad company never freezes the entire queue.
    Failures go to _handle_retry() — re-enqueued after delay, max 2 tries.
    After all retries exhausted → give up, reset, wait for next schedule.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
FULL FLOW DIAGRAM
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  APScheduler fires (every N min/hour/day)
          │
          ▼
  enqueue(company_name)
          │
          ├─ RULE 1: already running?        → SKIP (log)
          ├─ RULE 1: already in queue?       → SKIP (log)
          ├─ RULE 1: already ran this round? → SKIP (log)
          │
          ▼
  _round_active = True
  _round_companies.add(company_name)
  queue.Queue.put(company_name)   ← FIFO
          │
          ▼
  Worker Thread (single daemon)
          │
          ▼
  _process_company(company_name)
          │
          ├── TallyLauncher.prepare()   → fail? → return False (RULE 3)
          └── _run_sync()               → timeout? → return False (RULE 3)
          │
          ▼
  finally: _cleanup_after_company()    ← ALWAYS runs (success or fail)
          │
          ├─ success? → reset retry counter
          ├─ failed?  → _handle_retry() → re-enqueue after 60s (max 2x)
          │
          ▼
  queue empty?
          ├─ NO  → pick next company from queue
          └─ YES → RULE 2: _round_active = False
                           _round_companies.clear()
                           Round complete — gate reset ✓

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
QUEUE MESSAGES EMITTED TO GUI
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  ("sync_queue_started",  company_name)
  ("sync_queue_done",     company_name, success: bool)
  ("sync_queue_log",      company_name, message, level)
  ("sync_queue_progress", company_name, pct, label)
  ("queue_updated",       None)
  ("sync_overrun_detected", companies, elapsed_min, interval_min, suggested_min)
"""

import gc
import queue
import threading
import time
from datetime import datetime
from typing import Optional

from gui.state import AppState, CompanyStatus
from logging_config import logger


# ─────────────────────────────────────────────────────────────────────────────
#  Constants
# ─────────────────────────────────────────────────────────────────────────────
MAX_RETRIES         = 2    # Max retry attempts per company per round
RETRY_DELAY_SEC     = 60   # Seconds to wait before re-enqueuing a failed company
TIMEOUT_NO_MSG_SEC  = 300  # 5 min max silence in _run_sync before forcing exit
CLEANUP_SLEEP_SEC   = 3    # Seconds to pause between companies (Tally settle time)


class SyncQueueController:
    """
    Single instance per app session.

    Call start()              → once at app launch
    Call enqueue(name)        → from APScheduler jobs or manual triggers
    Call shutdown()           → on app close

    Enforces all three queue rules automatically.
    No caller needs to know about rounds or duplicate detection.
    """

    def __init__(self, state: AppState, app_queue: queue.Queue):
        self._state  = state
        self._app_q  = app_queue        # GUI queue polled by app.py
        self._queue  = queue.Queue()    # Internal FIFO queue
        self._lock   = threading.Lock()
        self._running = False
        self._worker_t: Optional[threading.Thread] = None

        # ── Public readable state (UI reads these) ────────────────────────
        self.current_company: Optional[str] = None   # company running RIGHT NOW
        self.queued_companies: list[str]    = []     # companies waiting in queue

        # ── RULE 2: Round Gate ────────────────────────────────────────────
        # _round_active  → True from first enqueue until queue fully empties
        # _round_companies → set of every company added in the current round
        #   (includes: already ran, currently running, waiting in queue)
        #   Used by RULE 1 to block re-adds mid-round
        self._round_active:    bool      = False
        self._round_companies: set[str]  = set()

        # ── Round timing (for overrun detection) ──────────────────────────
        self._round_start:        float      = 0.0
        self._round_company_list: list[str]  = []   # ordered list for overrun log

        # ── RULE 3: Retry counter ─────────────────────────────────────────
        # { company_name: retry_count }  — resets on success or exhaustion
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
            target = self._worker_loop,
            daemon = True,
            name   = "SyncQueueWorker",
        )
        self._worker_t.start()
        logger.info("[SyncQueue] Worker thread started ✓")

    def shutdown(self):
        """Signal worker to stop. Call on app close."""
        with self._lock:
            self._running = False
        self._queue.put(None)   # Unblock worker if waiting on empty queue
        logger.info("[SyncQueue] Shutdown signal sent")

    # ─────────────────────────────────────────────────────────────────────────
    #  Public API
    # ─────────────────────────────────────────────────────────────────────────

    def enqueue(self, company_name: str):
        """
        Add a company to the sync queue. Non-blocking.
        Called by APScheduler job functions and manual sync triggers.

        Enforces all three rules atomically inside a single lock block.
        No caller needs to check for duplicates — this method handles it.

        RULE 1 — Three-state duplicate check:
          1. Is company running RIGHT NOW?          → skip
          2. Is company WAITING in queue?           → skip
          3. Did company ALREADY RUN this round?    → skip

        RULE 2 — Round Gate:
          Sets _round_active = True on first add.
          _round_companies tracks every company in this round.

        All three checks + the append are inside ONE lock block.
        This eliminates any race condition between two scheduler threads
        firing at the same millisecond.
        """
        with self._lock:

            # ── RULE 1 Check 1: already running right now ─────────────────
            if company_name == self.current_company:
                logger.info(
                    f"[SyncQueue] SKIP '{company_name}' — currently syncing (Rule 1)"
                )
                return

            # ── RULE 1 Check 2: already waiting in queue ──────────────────
            if company_name in self.queued_companies:
                logger.info(
                    f"[SyncQueue] SKIP '{company_name}' — already queued (Rule 1)"
                )
                return

            # ── RULE 1 Check 3: already ran in this round ─────────────────
            # This is the KEY new check — blocks re-adds mid-round even when
            # the company is not currently in the queue or running.
            # Example: Company A ran at T=0, finished at T=5.
            # At T=10 scheduler fires again — round is still active (Company B
            # still running). Without this check, A would be re-added.
            if self._round_active and company_name in self._round_companies:
                logger.info(
                    f"[SyncQueue] SKIP '{company_name}' — already completed "
                    f"this round (Rule 1 + Rule 2)"
                )
                return

            # ── RULE 2: Mark round as active, track this company ──────────
            if not self._round_active:
                # First company of a new round — start round clock
                self._round_active        = True
                self._round_start         = time.time()
                self._round_company_list  = []
                logger.info(
                    f"[SyncQueue] ━━━ NEW ROUND STARTED ━━━ "
                    f"First company: '{company_name}'"
                )

            self._round_companies.add(company_name)
            self.queued_companies.append(company_name)

        # Put on queue AFTER releasing lock — queue.put is thread-safe
        self._queue.put(company_name)

        logger.info(
            f"[SyncQueue] Enqueued: '{company_name}' | "
            f"Queue: {self.queued_companies} | "
            f"Round: {self._round_companies}"
        )
        self._post("sync_queue_log", company_name, "⏳ Queued for sync", "INFO")
        self._app_q.put(("queue_updated", None))

    @property
    def is_idle(self) -> bool:
        """True when nothing is running and queue is empty."""
        return self.current_company is None and self._queue.empty()

    @property
    def queue_size(self) -> int:
        return self._queue.qsize()

    @property
    def round_active(self) -> bool:
        """True if a round is currently in progress."""
        return self._round_active

    @property
    def round_companies(self) -> set:
        """Read-only snapshot of companies in the current round."""
        with self._lock:
            return set(self._round_companies)

    # ─────────────────────────────────────────────────────────────────────────
    #  Worker Loop
    # ─────────────────────────────────────────────────────────────────────────

    def _worker_loop(self):
        """
        Single daemon thread — picks one company at a time from the queue.
        Blocks on queue.get() when idle — zero CPU waste.
        One company fully completes before the next one starts.
        RULE 3 is enforced here: any failure → skip → continue, never block.
        """
        logger.info("[SyncQueue] Worker loop started — waiting for jobs...")

        while True:
            # Block until a company arrives (or None for shutdown)
            company_name = self._queue.get()

            # ── Shutdown signal ───────────────────────────────────────────
            if company_name is None:
                logger.info("[SyncQueue] Worker loop shutting down")
                self._queue.task_done()
                break

            # ── Check if shutdown was called while waiting ────────────────
            with self._lock:
                if not self._running:
                    self._queue.task_done()
                    break
                # Move from "waiting" to "running"
                self.current_company = company_name
                if company_name in self.queued_companies:
                    self.queued_companies.remove(company_name)

            logger.info(
                f"[SyncQueue] ─── Processing: '{company_name}' "
                f"| Remaining in queue: {self._queue.qsize()} ───"
            )
            self._app_q.put(("queue_updated", None))

            # ── Process the company (RULE 3: all errors caught here) ──────
            company_start = time.time()
            success       = False

            try:
                success = self._process_company(company_name)

            except Exception as e:
                # Absolute last-resort catch — should never reach here
                # because _process_company has its own try/except.
                # But if it does, log it and continue — never block queue.
                logger.exception(
                    f"[SyncQueue] UNHANDLED ERROR for '{company_name}': {e}"
                )
                self._post(
                    "sync_queue_log", company_name,
                    f"✗ Unexpected error: {e}", "ERROR"
                )
                self._state.set_company_status(company_name, CompanyStatus.SYNC_ERROR)
                success = False

            finally:
                # ── Always runs — success or fail ─────────────────────────
                company_elapsed = time.time() - company_start

                # Clear current_company BEFORE cleanup sleep
                # so UI shows "idle" immediately
                with self._lock:
                    self.current_company = None

                # Signal queue task done
                self._queue.task_done()

                # Notify GUI
                self._app_q.put(("queue_updated", None))
                self._app_q.put(("sync_queue_done", company_name, success))

                logger.info(
                    f"[SyncQueue] Finished: '{company_name}' "
                    f"({'✓ OK' if success else '✗ FAILED'}) "
                    f"in {company_elapsed:.0f}s "
                    f"| Remaining: {self._queue.qsize()}"
                )

                # ── Memory cleanup between companies ──────────────────────
                # Runs after every company regardless of success/fail.
                # Prevents memory buildup across a long round.
                self._cleanup_after_company(company_name)

            # ── Track this company in the round list (for overrun log) ────
            self._round_company_list.append(company_name)

            # ── Retry on failure (RULE 3) ─────────────────────────────────
            if success:
                # Reset retry counter on success
                with self._lock:
                    if self._retry_counts.get(company_name, 0) > 0:
                        self._retry_counts[company_name] = 0
                        logger.debug(
                            f"[SyncQueue] '{company_name}' success — retry counter reset"
                        )
            else:
                # Failed — attempt retry (re-enqueue after delay)
                self._handle_retry(company_name)

            # ── RULE 2: Check if round is complete ────────────────────────
            # Queue is empty → all companies in this round have been processed.
            # Reset the Round Gate so the next scheduler fire starts a new round.
            if self._queue.empty():
                self._on_round_complete()

    # ─────────────────────────────────────────────────────────────────────────
    #  RULE 2 — Round complete handler
    # ─────────────────────────────────────────────────────────────────────────

    def _on_round_complete(self):
        """
        Called when queue goes empty — end of a round.

        Steps:
          1. Log round summary
          2. Check for overrun
          3. Reset Round Gate (_round_active = False, _round_companies.clear())

        After reset, the next scheduler fire will start a fresh round.
        Any company that was skipped mid-round will be eligible again.
        """
        round_elapsed     = time.time() - self._round_start
        round_elapsed_min = round_elapsed / 60
        company_count     = len(self._round_company_list)

        logger.info(
            f"[SyncQueue] ━━━ ROUND COMPLETE ━━━ "
            f"{company_count} company(s) processed in "
            f"{round_elapsed_min:.1f} min | "
            f"Companies: {self._round_company_list}"
        )

        # Check if round took longer than the shortest interval
        self._check_round_overrun(round_elapsed)

        # ── Reset Round Gate ──────────────────────────────────────────────
        with self._lock:
            self._round_active       = False
            self._round_companies.clear()
            self._round_company_list = []
            self._round_start        = 0.0

        logger.info(
            "[SyncQueue] Round Gate reset — ready for next round ✓"
        )

    # ─────────────────────────────────────────────────────────────────────────
    #  Overrun detection
    # ─────────────────────────────────────────────────────────────────────────

    def _check_round_overrun(self, round_elapsed_sec: float):
        """
        Compare round duration against the shortest scheduled interval
        of any company that ran in this round.

        If round took longer → log WARNING + post event to GUI.
        GUI can show amber banner: "Increase interval to at least N min"

        Example:
          6 companies, all every 10 min
          Round took 18 min
          → WARNING: overrun by 8 min, suggest 22 min minimum
        """
        if not self._round_company_list:
            return

        round_elapsed_min     = round_elapsed_sec / 60
        shortest_interval_sec = None
        shortest_company      = None

        for name in self._round_company_list:
            co = self._state.get_company(name)
            if not co or not getattr(co, "schedule_enabled", False):
                continue

            interval = getattr(co, "schedule_interval", "hourly")
            value    = max(1, int(getattr(co, "schedule_value", 1)))

            if interval == "minutes":
                interval_sec = value * 60
            elif interval == "hourly":
                interval_sec = value * 3600
            else:
                continue   # daily — no overrun concern

            if shortest_interval_sec is None or interval_sec < shortest_interval_sec:
                shortest_interval_sec = interval_sec
                shortest_company      = name

        if shortest_interval_sec is None:
            return

        shortest_interval_min = shortest_interval_sec / 60

        if round_elapsed_sec > shortest_interval_sec:
            overrun_min   = round_elapsed_min - shortest_interval_min
            suggested_min = int(round_elapsed_min * 1.2)   # 20% buffer

            logger.warning(
                f"[SyncQueue] ⚠ OVERRUN — "
                f"Round: {round_elapsed_min:.1f}min | "
                f"Shortest interval: {shortest_interval_min:.0f}min | "
                f"Overrun by: {overrun_min:.1f}min | "
                f"Suggest minimum: {suggested_min}min"
            )

            # Post to GUI for amber banner display
            self._app_q.put((
                "sync_overrun_detected",
                list(self._round_company_list),
                round_elapsed_min,
                shortest_interval_min,
                suggested_min,
            ))
        else:
            logger.info(
                f"[SyncQueue] Round within interval ✓ — "
                f"{round_elapsed_min:.1f}min / {shortest_interval_min:.0f}min"
            )

    # ─────────────────────────────────────────────────────────────────────────
    #  RULE 3 — Retry handler
    # ─────────────────────────────────────────────────────────────────────────

    def _handle_retry(self, company_name: str):
        """
        On failure: re-enqueue after RETRY_DELAY_SEC (max MAX_RETRIES times).
        Re-enqueue happens in a background thread so the worker loop
        is NOT blocked during the wait — other companies continue normally.

        After all retries exhausted → reset counter, give up until next schedule.

        NOTE: Retried companies DO bypass the Round Gate check — they are
        re-added even if _round_active is True. This is intentional because
        the retry is for the same round's failure, not a new scheduler trigger.
        The _round_companies set is NOT updated again (company already in it).
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

            def _delayed_retry(name=company_name):
                time.sleep(RETRY_DELAY_SEC)
                logger.info(f"[SyncQueue] Re-enqueuing '{name}' after retry delay")
                # Direct queue inject — bypasses Round Gate (intentional for retries)
                with self._lock:
                    if name not in self.queued_companies and name != self.current_company:
                        self.queued_companies.append(name)
                self._queue.put(name)
                self._app_q.put(("queue_updated", None))

            threading.Thread(
                target = _delayed_retry,
                daemon = True,
                name   = f"RetryDelay-{company_name}",
            ).start()

        else:
            # All retries exhausted
            with self._lock:
                self._retry_counts[company_name] = 0

            logger.error(
                f"[SyncQueue] '{company_name}' failed after "
                f"{MAX_RETRIES} retries — giving up until next schedule"
            )
            self._post(
                "sync_queue_log", company_name,
                f"✗ Sync failed after {MAX_RETRIES} retries — "
                f"will retry at next scheduled time",
                "ERROR",
            )

    # ─────────────────────────────────────────────────────────────────────────
    #  Process one company — full pipeline
    # ─────────────────────────────────────────────────────────────────────────

    def _process_company(self, company_name: str) -> bool:
        """
        Full pipeline for one company:
          Phase A — TallyLauncher.prepare()  (open Tally + switch company)
          Phase B — _run_sync()              (XML pull + MySQL write)

        Returns True on success, False on ANY failure.
        RULE 3: every failure path returns False — never raises to caller.
        The worker loop then decides retry or skip.
        """
        self._post("sync_queue_started", company_name)
        self._state.set_company_status(company_name, CompanyStatus.SYNCING)

        # ── Phase A: Tally Automation ─────────────────────────────────────
        self._post(
            "sync_queue_log", company_name,
            "🤖 Starting Tally automation...", "INFO"
        )

        try:
            from services.tally_launcher import TallyLauncher
            launcher = TallyLauncher(self._state)
            ok, msg  = launcher.prepare(company_name)

            if not ok:
                logger.error(f"[SyncQueue] '{company_name}' automation failed: {msg}")
                self._post(
                    "sync_queue_log", company_name,
                    f"✗ Tally automation failed: {msg}", "ERROR"
                )
                self._state.set_company_status(company_name, CompanyStatus.SYNC_ERROR)
                return False   # RULE 3: skip, do not block queue

            self._post(
                "sync_queue_log", company_name,
                "✓ Tally ready — starting sync...", "SUCCESS"
            )

        except Exception as e:
            logger.exception(f"[SyncQueue] '{company_name}' TallyLauncher error: {e}")
            self._post(
                "sync_queue_log", company_name,
                f"✗ TallyLauncher error: {e}", "ERROR"
            )
            self._state.set_company_status(company_name, CompanyStatus.SYNC_ERROR)
            return False   # RULE 3: skip, do not block queue

        # ── Phase B: XML Sync ─────────────────────────────────────────────
        return self._run_sync(company_name)

    def _run_sync(self, company_name: str) -> bool:
        """
        Run SyncController for one company and BLOCK until fully complete.

        Auto-detects sync mode:
          is_initial_done = False → SNAPSHOT (full date range pull)
          is_initial_done = True  → INCREMENTAL (alter_id CDC)

        Exit conditions (RULE 3 — never hangs):
          1. "all_done" event received from SyncController  ← normal exit
          2. TIMEOUT_NO_MSG_SEC of silence                  ← safety net exit

        We do NOT use sync_active as an exit condition — it can be False
        for unrelated reasons and would cause premature exit.
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
                f"⚠ No initial snapshot — running full snapshot from {from_date}",
                "WARNING",
            )
        else:
            sync_mode = SyncMode.INCREMENTAL
            from_date = None
            self._post(
                "sync_queue_log", company_name,
                "→ Running incremental sync", "INFO"
            )

        # ── Start SyncController ──────────────────────────────────────────
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
                    # Normal exit — SyncController confirmed it finished
                    logger.info(
                        f"[SyncQueue] '{company_name}' sync all_done received ✓"
                    )
                    break

            except queue.Empty:
                # Safety net — only timeout exits, never sync_active check
                elapsed = time.time() - last_msg_time
                if elapsed > TIMEOUT_NO_MSG_SEC:
                    logger.error(
                        f"[SyncQueue] '{company_name}' timed out — "
                        f"no message for {TIMEOUT_NO_MSG_SEC}s — forcing exit"
                    )
                    success = False
                    break
                # Otherwise keep waiting — sync is still running

        return success

    # ─────────────────────────────────────────────────────────────────────────
    #  Memory cleanup between companies
    # ─────────────────────────────────────────────────────────────────────────

    def _cleanup_after_company(self, company_name: str):
        """
        Called after EVERY company finishes — success or fail.
        Releases memory before the next company starts.

        Three steps:
          1. Python GC — clears circular references and lingering objects
          2. Windows memory trim — releases RAM pages back to OS immediately
             (without this, Windows keeps the pages cached even after gc)
          3. Sleep CLEANUP_SLEEP_SEC — gives Tally time to settle before
             the next company's automation begins

        This prevents memory buildup across a long round of 10+ companies.
        Without this, by Company 6-7 the process RAM is bloated and
        PyAutoGUI screenshot calls start slowing down or failing.
        """
        logger.debug(f"[SyncQueue] Cleanup after '{company_name}'...")

        # Step 1: Python garbage collection
        collected = gc.collect()
        logger.debug(f"[SyncQueue] GC collected {collected} objects")

        # Step 2: Windows — trim process working set
        # Forces Windows to release cached RAM pages to the OS
        try:
            import ctypes
            # -1 as handle = current process
            ctypes.windll.kernel32.SetProcessWorkingSetSize(-1, -1)
            logger.debug("[SyncQueue] Windows memory trim done")
        except Exception:
            pass  # Non-Windows or permission denied — skip silently

        # Step 3: Settle time — lets Tally finish any pending UI operations
        # before the next company's PyAutoGUI automation starts clicking
        logger.debug(
            f"[SyncQueue] Waiting {CLEANUP_SLEEP_SEC}s for Tally to settle..."
        )
        time.sleep(CLEANUP_SLEEP_SEC)

        logger.info(f"[SyncQueue] Cleanup done after '{company_name}' ✓")

    # ─────────────────────────────────────────────────────────────────────────
    #  Helper
    # ─────────────────────────────────────────────────────────────────────────

    def _post(self, *args):
        """Put a message on the GUI app queue."""
        self._app_q.put(args)