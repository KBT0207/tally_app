"""
gui/controllers/missed_sync_checker.py
========================================
Phase 2 — Missed Sync Catch-up

Runs once on every app startup AFTER companies and scheduler config
have been loaded from the DB.

Logic per company (only if schedule_enabled = True):
  ┌─────────────────┬────────────────────────────────────────────────┐
  │ Interval type   │ Missed if...                                   │
  ├─────────────────┼────────────────────────────────────────────────┤
  │ minutes         │ last_sync_time > 2× interval minutes ago       │
  │ hourly          │ last_sync_time > N hours ago (grace: +15 min)  │
  │ daily           │ last_sync_time is not from today               │
  └─────────────────┴────────────────────────────────────────────────┘

Never missed if:
  - schedule_enabled = False
  - last_sync_time is None  (never synced — initial snapshot handles this)
  - company is currently syncing or queued

Output:
  - Returns list of company names that were missed
  - Calls sync_queue_controller.enqueue() for each missed company
  - Posts a single "missed_syncs_found" event to the GUI queue
    so app.py can show a notification banner

Usage (from app.py after startup):
    from gui.controllers.missed_sync_checker import MissedSyncChecker

    checker = MissedSyncChecker(
        state                = self.state,
        sync_queue_controller = self._sync_queue_controller,
        app_queue            = self._q,
    )
    missed = checker.check_and_enqueue()
    # missed = ["CompanyA", "CompanyB"]  ← shown in UI notification
"""

from datetime import datetime, timedelta
from typing import Optional

from gui.state import AppState, CompanyState
from logging_config import logger


# ─────────────────────────────────────────────────────────────────────────────
#  Grace periods — prevent false positives on borderline cases
# ─────────────────────────────────────────────────────────────────────────────
# Minutes: missed if elapsed > interval + 30s grace
# (e.g. every-1-min job: missed if last sync was > 90s ago)
# Using a flat 30s grace instead of 2× factor so a 1-min job isn't
# considered missed only after 2 full minutes have passed.
MINUTES_GRACE_SECONDS = 30    # flat grace for minutes-interval companies
HOURLY_GRACE_MINUTES  = 15    # hourly company: add 15 min grace to interval
DAILY_CUTOFF_HOUR     = 4     # daily company: don't catch up between midnight–4am


class MissedSyncChecker:
    """
    Single-use checker — create one instance per app startup, call check_and_enqueue().
    """

    def __init__(
        self,
        state:                  AppState,
        sync_queue_controller,              # SyncQueueController instance
        app_queue,                          # queue.Queue — GUI queue in app.py
    ):
        self._state   = state
        self._queue   = sync_queue_controller
        self._app_q   = app_queue

    # ─────────────────────────────────────────────────────────────────────────
    #  Main entry point
    # ─────────────────────────────────────────────────────────────────────────
    def check_and_enqueue(self) -> list[str]:
        """
        Check every scheduled company for missed syncs.
        Enqueue missed ones into SyncQueueController.
        Returns list of missed company names (may be empty).

        Safe to call from main thread — does no blocking I/O.

        KEY RULE: MissedSyncChecker is the ONLY catch-up mechanism.
        APScheduler misfire_grace_time=1 means it never fires catch-up jobs.
        So this method runs ONCE on startup and enqueues missed companies exactly once.
        The SyncQueueController duplicate-check is a final safety net.
        """
        now    = datetime.now()
        missed = []

        for name, co in self._state.companies.items():

            # ── Only check scheduled companies ───────────────────────────
            if not getattr(co, "schedule_enabled", False):
                continue

            # ── Skip if never synced — initial snapshot handles first run
            if co.last_sync_time is None:
                logger.debug(
                    f"[MissedSync] '{name}' has no last_sync_time — skipping "
                    f"(initial snapshot will handle first run)"
                )
                continue

            # ── Skip if already running, queued, or in active round ───────
            # Three-state guard — mirrors the same checks in enqueue().
            # Even though MissedSyncChecker now runs BEFORE the scheduler
            # starts (correct startup order), this guard stays as a safety
            # net in case the order ever changes or a retry is in progress.
            if name == self._queue.current_company:
                logger.debug(f"[MissedSync] '{name}' already syncing — skipping")
                continue
            if name in self._queue.queued_companies:
                logger.debug(f"[MissedSync] '{name}' already in queue — skipping")
                continue
            # New: check Round Gate — if a round is active and this company
            # already ran (e.g. a retry fired it), don't double-enqueue.
            if self._queue.round_active and name in self._queue.round_companies:
                logger.debug(
                    f"[MissedSync] '{name}' already in active round — skipping"
                )
                continue

            # ── Check if this company was missed ─────────────────────────
            was_missed, reason = self._is_missed(co, now)

            if was_missed:
                missed.append(name)
                logger.info(
                    f"[MissedSync] ✗ MISSED: '{name}' — {reason} — enqueuing now"
                )
                self._queue.enqueue(name)
            else:
                logger.debug(f"[MissedSync] ✓ OK: '{name}' — {reason}")

        # ── Notify GUI ────────────────────────────────────────────────────
        if missed:
            self._app_q.put(("missed_syncs_found", missed))
            logger.info(
                f"[MissedSync] {len(missed)} missed sync(s) enqueued: {missed}"
            )
        else:
            logger.info("[MissedSync] No missed syncs found")

        return missed

    # ─────────────────────────────────────────────────────────────────────────
    #  Per-company missed logic
    # ─────────────────────────────────────────────────────────────────────────
    def _is_missed(self, co: CompanyState, now: datetime) -> tuple[bool, str]:
        """
        Returns (missed: bool, reason: str) for one company.
        reason is a human-readable explanation for logging.
        """
        last     = co.last_sync_time
        interval = getattr(co, "schedule_interval", "hourly")
        value    = max(1, int(getattr(co, "schedule_value", 1)))
        time_str = getattr(co, "schedule_time", "09:00")

        # ── Minutes interval ──────────────────────────────────────────────
        if interval == "minutes":
            threshold = timedelta(minutes=value, seconds=MINUTES_GRACE_SECONDS)
            elapsed   = now - last
            missed    = elapsed > threshold
            reason    = (
                f"last sync {_fmt_elapsed(elapsed)} ago, "
                f"threshold {value}m + {MINUTES_GRACE_SECONDS}s grace"
            )
            return missed, reason

        # ── Hourly interval ───────────────────────────────────────────────
        elif interval == "hourly":
            threshold = timedelta(hours=value, minutes=HOURLY_GRACE_MINUTES)
            elapsed   = now - last
            missed    = elapsed > threshold
            reason    = (
                f"last sync {_fmt_elapsed(elapsed)} ago, "
                f"threshold {value}h {HOURLY_GRACE_MINUTES}m grace"
            )
            return missed, reason

        # ── Daily interval ────────────────────────────────────────────────
        elif interval == "daily":
            # Don't catch up in the early hours (e.g. midnight–4am)
            # — the scheduled time probably hasn't arrived yet today
            if now.hour < DAILY_CUTOFF_HOUR:
                return False, f"daily — before {DAILY_CUTOFF_HOUR:02d}:00 cutoff, skip"

            # Parse scheduled time
            try:
                h, m = map(int, time_str.split(":"))
            except Exception:
                h, m = 9, 0

            # Build "when it should have run today"
            scheduled_today = now.replace(hour=h, minute=m, second=0, microsecond=0)

            # Last sync was today at or after the scheduled time → not missed
            if last.date() == now.date() and last >= scheduled_today:
                return False, f"daily — already synced today at {last.strftime('%H:%M')}"

            # Last sync was today but before the scheduled time,
            # and scheduled time is in the future → not missed yet
            if last.date() == now.date() and now < scheduled_today:
                return False, f"daily — scheduled at {time_str}, not due yet"

            # Last sync was yesterday or earlier, and scheduled time has passed today
            if now >= scheduled_today:
                missed_days = (now.date() - last.date()).days
                return True, f"daily — last sync {missed_days} day(s) ago, due at {time_str}"

            return False, "daily — not due yet"

        # ── Unknown interval — skip safely ────────────────────────────────
        return False, f"unknown interval '{interval}' — skipping"


# ─────────────────────────────────────────────────────────────────────────────
#  Helpers
# ─────────────────────────────────────────────────────────────────────────────
def _fmt_elapsed(td: timedelta) -> str:
    """Format a timedelta into a readable string like '2h 15m' or '45m'."""
    total_sec = int(td.total_seconds())
    if total_sec < 0:
        return "0m"
    h = total_sec // 3600
    m = (total_sec % 3600) // 60
    if h > 0:
        return f"{h}h {m}m"
    return f"{m}m"