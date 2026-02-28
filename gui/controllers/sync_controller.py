"""
gui/controllers/sync_controller.py
=====================================
Bridges the GUI sync page with the existing sync_service.py.

Phase 2 fix:
  - sync_active flag is now set/cleared under a threading.Lock
    to prevent race condition when scheduler fires at the same time
    as a manual sync completing.

Thread model:
  GUI thread  ──────────────────────────────────────────────►
                │  start_sync()                             │
                │  ↓ spawns SyncThread                     │
  SyncThread  ──►  runs sync_company()                     │
                │  ↓ puts msgs in queue                    │
  GUI thread  ◄──  polls queue every 100ms via .after()    │

Queue message format (tuples):
  ("log",      company, message, level)
  ("progress", company, pct, label)
  ("status",   company, status_str)
  ("done",     company, success_bool)
  ("all_done",)
"""

import threading
import queue
from datetime import datetime
from typing import Optional

from gui.state  import AppState, CompanyStatus, SyncMode, VoucherSelection
from logging_config import logger


# ── Module-level lock shared across all SyncController instances ──────────────
# Prevents two syncs (manual + scheduler) from running simultaneously
_SYNC_LOCK = threading.Lock()


# Voucher types in the order they sync (matches VOUCHER_CONFIG in sync_service)
VOUCHER_ORDER = [
    "ledger",
    "items",
    "trial_balance",
    "sales",
    "purchase",
    "credit_note",
    "debit_note",
    "receipt",
    "payment",
    "journal",
    "contra",
]


class SyncController:
    """
    Instantiated once per sync run.
    Call start() to kick off, cancel() to request stop.
    Pass a queue.Queue that the GUI polls for messages.
    """

    def __init__(
        self,
        state:          AppState,
        out_queue:      queue.Queue,
        companies:      list[str],
        sync_mode:      str,
        from_date:      Optional[str],
        to_date:        str,
        vouchers,
        sequential:     bool = True,
        from_dates_map: Optional[dict] = None,
        to_dates_map:   Optional[dict] = None,
    ):
        self._state      = state
        self._q          = out_queue
        self._companies  = companies
        self._sync_mode  = sync_mode
        self._from_date  = from_date
        self._to_date    = to_date
        self._from_dates = from_dates_map or {n: from_date for n in companies}
        self._to_dates   = to_dates_map   or {n: to_date   for n in companies}

        if isinstance(vouchers, dict):
            self._vouchers_map = vouchers
        else:
            self._vouchers_map = {n: vouchers for n in companies}

        self._sequential = sequential
        self._cancelled  = False
        self._threads: list[threading.Thread] = []

    # ─────────────────────────────────────────────────────────────────────────
    #  Public API
    # ─────────────────────────────────────────────────────────────────────────
    def start(self):
        """
        Start the sync. Returns immediately — work happens in background.

        Phase 2: Uses module-level lock to prevent double-sync race condition.
        If another sync is already running, logs a warning and aborts.
        """
        with _SYNC_LOCK:
            if self._state.sync_active:
                logger.warning("[SyncController] Sync already active — skipping duplicate start")
                # Post all_done so caller doesn't hang waiting
                self._q.put(("all_done",))
                return
            self._state.sync_active    = True
            self._state.sync_cancelled = False

        if self._sequential:
            t = threading.Thread(target=self._run_sequential, daemon=True)
            t.start()
            self._threads = [t]
        else:
            self._run_parallel()

    def cancel(self):
        """Signal all threads to stop after current operation."""
        self._cancelled            = True
        self._state.sync_cancelled = True
        self._log_all("Cancellation requested — stopping after current step...", "WARNING")

    # ─────────────────────────────────────────────────────────────────────────
    #  Sequential run
    # ─────────────────────────────────────────────────────────────────────────
    def _run_sequential(self):
        total = len(self._companies)
        for idx, name in enumerate(self._companies):
            if self._cancelled:
                break
            self._post("log", name, f"[{idx+1}/{total}] Starting sync for {name}", "INFO")
            self._sync_one(name)

        self._finish()

    # ─────────────────────────────────────────────────────────────────────────
    #  Parallel run
    # ─────────────────────────────────────────────────────────────────────────
    def _run_parallel(self):
        threads = []
        for name in self._companies:
            t = threading.Thread(target=self._sync_one, args=(name,), daemon=True)
            threads.append(t)
            self._threads = threads

        for t in threads:
            t.start()

        def watcher():
            for t in threads:
                t.join()
            self._finish()

        threading.Thread(target=watcher, daemon=True).start()

    # ─────────────────────────────────────────────────────────────────────────
    #  Sync one company
    # ─────────────────────────────────────────────────────────────────────────
    def _sync_one(self, company_name: str):
        engine = self._state.db_engine
        if not engine:
            self._post("log",    company_name, "No DB engine — cannot sync", "ERROR")
            self._post("status", company_name, CompanyStatus.SYNC_ERROR)
            self._post("done",   company_name, False)
            return

        self._post("status",   company_name, CompanyStatus.SYNCING)
        self._post("progress", company_name, 0.0, "Connecting to Tally...")

        # ── Connect to Tally ──────────────────────────────
        try:
            from services.tally_connector import TallyConnector
            co_state = self._state.get_company(company_name)
            host = co_state.tally_host if co_state else self._state.tally.host
            port = co_state.tally_port if co_state else self._state.tally.port

            tally = TallyConnector(host=host, port=port)
            if tally.status != "Connected":
                raise ConnectionError(f"Tally not reachable at {host}:{port}")

            self._post("log",      company_name, "✓ Tally connected", "SUCCESS")
            self._post("progress", company_name, 5.0, "Tally connected")

        except Exception as e:
            self._post("log",    company_name, f"✗ Tally connection failed: {e}", "ERROR")
            self._post("status", company_name, CompanyStatus.TALLY_OFFLINE)
            self._post("done",   company_name, False)
            return

        if self._cancelled:
            self._post("log",    company_name, "Cancelled before sync started", "WARNING")
            self._post("status", company_name, CompanyStatus.CONFIGURED)
            self._post("done",   company_name, False)
            return

        # ── Determine dates ───────────────────────────────
        company_dict = self._build_company_dict(company_name)
        co_state     = self._state.get_company(company_name)

        from_date = self._from_dates.get(company_name, self._from_date)
        to_date   = self._to_dates.get(company_name, self._to_date)

        if self._sync_mode == SyncMode.INCREMENTAL:
            if co_state and not co_state.is_initial_done:
                self._post("log", company_name,
                    "⚠  Initial sync not done yet — running full snapshot first.", "WARNING")
                from_date = co_state.starting_from or company_dict.get("starting_from")
            else:
                from_date = None

        if self._sync_mode == SyncMode.SNAPSHOT and not from_date:
            from_date = (co_state.starting_from if co_state else None) \
                        or company_dict.get("starting_from", "20240401")
            self._post("log", company_name,
                f"ℹ  No from date specified — using company default: {from_date}", "INFO")

        if from_date:
            self._post("log", company_name, f"📅  Date range: {from_date} → {to_date}", "INFO")
        else:
            self._post("log", company_name, f"📅  Incremental (alter_id) up to {to_date}", "INFO")

        # ── Determine vouchers ────────────────────────────
        voucher_sel = self._vouchers_map.get(
            company_name,
            next(iter(self._vouchers_map.values())) if self._vouchers_map else VoucherSelection()
        )
        selected = voucher_sel.selected_types()
        self._post("log", company_name, f"Syncing: {', '.join(selected)}", "INFO")

        # ── Run sync ──────────────────────────────────────
        try:
            total_steps = len(selected)
            done_steps  = 0

            from services.sync_service import (
                VOUCHER_CONFIG, _sync_ledgers, _sync_items,
                _sync_trial_balance, _sync_voucher
            )

            if "ledger" in selected:
                if self._cancelled:
                    raise InterruptedError("Cancelled")
                self._post("progress", company_name, 10.0, "Syncing ledgers...")
                self._post("log",      company_name, "→ Ledgers", "INFO")
                _sync_ledgers(company_name, tally, engine)
                done_steps += 1
                pct = 10 + (done_steps / total_steps) * 80
                self._post("progress", company_name, pct, "Ledgers done")
                self._post("log",      company_name, "✓ Ledgers done", "SUCCESS")

            if "items" in selected:
                if self._cancelled:
                    raise InterruptedError("Cancelled")
                pct = 10 + (done_steps / max(total_steps, 1)) * 80
                self._post("progress", company_name, pct, "Syncing items...")
                self._post("log",      company_name, "→ Items (StockItem master)", "INFO")
                _sync_items(company_name, tally, engine)
                done_steps += 1
                self._post("log", company_name, "✓ Items done", "SUCCESS")

            if "trial_balance" in selected:
                if self._cancelled:
                    raise InterruptedError("Cancelled")
                pct = 10 + (done_steps / max(total_steps, 1)) * 80
                self._post("progress", company_name, pct, "Syncing trial balance...")
                self._post("log",      company_name, "→ Trial Balance", "INFO")
                fd = from_date or company_dict.get('starting_from', '20240401')
                _sync_trial_balance(company_name, tally, engine, fd, to_date)
                done_steps += 1
                self._post("log", company_name, "✓ Trial Balance done", "SUCCESS")

            voucher_configs = [
                cfg for cfg in VOUCHER_CONFIG
                if cfg["voucher_type"] in selected
            ]

            for cfg in voucher_configs:
                if self._cancelled:
                    raise InterruptedError("Cancelled")

                vtype = cfg["voucher_type"]
                label = cfg["parser_type_name"]
                pct   = 10 + (done_steps / max(total_steps, 1)) * 80

                self._post("progress", company_name, pct,  f"Syncing {label}...")
                self._post("log",      company_name, f"→ {label}", "INFO")

                fd = from_date or company_dict.get('starting_from', '20240401')
                _sync_voucher(
                    company_name = company_name,
                    config       = cfg,
                    tally        = tally,
                    engine       = engine,
                    from_date    = fd,
                    to_date      = to_date,
                )

                done_steps += 1
                self._post("log", company_name, f"✓ {label} done", "SUCCESS")

            # ── Done ──────────────────────────────────────
            self._post("progress", company_name, 100.0, "Complete ✓")
            self._post("status",   company_name, CompanyStatus.SYNC_DONE)
            self._post("log",      company_name, f"✓ {company_name} sync complete", "SUCCESS")
            self._post("done",     company_name, True)

            self._state.set_company_status(
                company_name,
                CompanyStatus.SYNC_DONE,
                last_sync_time=datetime.now(),
            )

        except InterruptedError:
            self._post("log",    company_name, "Sync cancelled by user", "WARNING")
            self._post("status", company_name, CompanyStatus.CONFIGURED)
            self._post("done",   company_name, False)

        except Exception as e:
            logger.exception(f"[SyncController][{company_name}] Unexpected error")
            self._post("log",    company_name, f"✗ Error: {e}", "ERROR")
            self._post("status", company_name, CompanyStatus.SYNC_ERROR)
            self._post("done",   company_name, False)

    # ─────────────────────────────────────────────────────────────────────────
    #  Finish — Phase 2: clear sync_active under lock
    # ─────────────────────────────────────────────────────────────────────────
    def _finish(self):
        with _SYNC_LOCK:
            self._state.sync_active = False
        self._q.put(("all_done",))
        logger.info("[SyncController] All company syncs finished")

    # ─────────────────────────────────────────────────────────────────────────
    #  Helpers
    # ─────────────────────────────────────────────────────────────────────────
    def _post(self, *args):
        self._q.put(args)

    def _log_all(self, message: str, level: str = "INFO"):
        for name in self._companies:
            self._post("log", name, message, level)

    def _build_company_dict(self, name: str) -> dict:
        co = self._state.get_company(name)
        if co:
            return {
                "name":          co.name,
                "starting_from": co.starting_from or "20240401",
            }
        return {"name": name, "starting_from": "20240401"}