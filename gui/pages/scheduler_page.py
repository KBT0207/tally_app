"""
gui/pages/scheduler_page.py
=============================
Scheduler page — per-company auto-sync configuration.

Layout:
┌─────────────────────────────────────────────────────────────────┐
│  Scheduler Status: ● Running   [+ Add Schedule]                 │
├─────────────────────────────────────────────────────────────────┤
│  ABC Traders Pvt Ltd                        [● Active]          │
│  Every: [1] [Hour(s) ▾]   Next run: 20 Feb 2026  11:30         │
│  Vouchers: All                    [Edit]  [Run Now]  [Disable]  │
├─────────────────────────────────────────────────────────────────┤
│  XYZ Enterprises                            [○ Disabled]        │
│  Every: [1] [Day ▾]  At: [09:00] [+ Add Time]  Next run: —    │
│  Vouchers: Sales, Purchase           [Edit]  [Run Now]  [Enable]│
└─────────────────────────────────────────────────────────────────┘

Edit opens an inline form that expands below the row.
For daily interval, multiple times can be added (e.g. 01:00 and 14:00).
"""

import tkinter as tk
from tkinter import messagebox
from datetime import datetime
from typing import Optional

from gui.state  import AppState, CompanyState, CompanyStatus
from gui.styles import Color, Font, Spacing


# ─────────────────────────────────────────────────────────────────────────────
#  Schedule row widget — one per company
# ─────────────────────────────────────────────────────────────────────────────
class ScheduleRow(tk.Frame):
    """
    Displays one company's schedule status.
    Expandable edit form opens inline.

    IMPORTANT: always reads `self._state.companies[self._name]` — never caches
    the CompanyState object — so that a Home-page Refresh (which creates brand-new
    CompanyState objects) is reflected here automatically.
    """

    INTERVALS = ["minutes", "hourly", "daily"]
    INTERVAL_LABELS = {
        "minutes": "Minute(s)",
        "hourly":  "Hour(s)",
        "daily":   "Daily at",
    }

    def __init__(
        self,
        parent,
        company:    CompanyState,
        controller,          # SchedulerController
        co_ctrl,             # CompanyController
        on_run_now,          # callback(name)
        state:      AppState = None,   # live state reference
        app         = None,            # TallySyncApp — for sync_queue_controller access
        **kwargs,
    ):
        super().__init__(parent, bg=Color.BG_CARD, **kwargs)
        self._name       = company.name   # key into state.companies
        self._state      = state          # live AppState — read company from here
        self._sched_ctrl = controller
        self._co_ctrl    = co_ctrl
        self._on_run_now = on_run_now
        self._page_app   = app            # used by _meta_text for round-aware label
        self._editing    = False

        # Multiple-time UI state
        self._time_entry_vars:  list  = []   # list of tk.StringVar (one per time row)
        self._time_entry_rows:  list  = []   # list of tk.Frame    (one per time row)

        self._build()

    # ── Live company accessor — always returns the current object ─────────────
    @property
    def company(self) -> CompanyState:
        """Always fetch from live state so stale references never happen."""
        if self._state and self._name in self._state.companies:
            return self._state.companies[self._name]
        # Fallback: return whatever was passed in originally
        return self._fallback_company

    @company.setter
    def company(self, value: CompanyState):
        """Keep a fallback copy in case state is not wired yet."""
        self._fallback_company = value
        self._name = value.name

    # ─────────────────────────────────────────────────────────────────────────
    def _build(self):
        self.columnconfigure(0, weight=1)

        # ── Summary row ───────────────────────────────────
        summary = tk.Frame(self, bg=Color.BG_CARD, padx=Spacing.LG, pady=Spacing.MD)
        summary.grid(row=0, column=0, sticky="ew")
        summary.columnconfigure(1, weight=1)
        self._summary = summary

        # Company name
        tk.Label(
            summary, text=self._name,
            font=Font.LABEL_BOLD, bg=Color.BG_CARD, fg=Color.TEXT_PRIMARY, anchor="w",
        ).grid(row=0, column=0, sticky="w")

        # Status badge
        self._status_lbl = tk.Label(
            summary,
            text=self._status_text(),
            font=Font.BADGE,
            bg=self._status_bg(),
            fg=self._status_fg(),
            padx=8, pady=2,
        )
        self._status_lbl.grid(row=0, column=2, padx=(Spacing.MD, Spacing.MD))

        # Buttons
        btn_frame = tk.Frame(summary, bg=Color.BG_CARD)
        btn_frame.grid(row=0, column=3)

        self._edit_btn = tk.Button(
            btn_frame, text="✎ Edit",
            font=Font.BUTTON_SM, bg=Color.BG_ROOT, fg=Color.TEXT_PRIMARY,
            relief="solid", bd=1, padx=8, pady=3,
            cursor="hand2", command=self._toggle_edit,
        )
        self._edit_btn.pack(side="left", padx=(0, Spacing.XS))

        tk.Button(
            btn_frame, text="▶ Run Now",
            font=Font.BUTTON_SM, bg=Color.PRIMARY, fg=Color.TEXT_WHITE,
            relief="flat", bd=0, padx=8, pady=3,
            cursor="hand2",
            command=lambda: self._on_run_now(self._name),
        ).pack(side="left", padx=(0, Spacing.XS))

        self._toggle_btn = tk.Button(
            btn_frame,
            text="Disable" if self.company.schedule_enabled else "Enable",
            font=Font.BUTTON_SM,
            bg=Color.DANGER_BG if self.company.schedule_enabled else Color.SUCCESS_BG,
            fg=Color.DANGER_FG if self.company.schedule_enabled else Color.SUCCESS_FG,
            relief="flat", bd=0, padx=8, pady=3,
            cursor="hand2",
            command=self._toggle_enable,
        )
        self._toggle_btn.pack(side="left")

        # Meta row (next run + schedule description)
        self._meta_lbl = tk.Label(
            summary, text=self._meta_text(),
            font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY, anchor="w",
        )
        self._meta_lbl.grid(row=1, column=0, columnspan=4, sticky="w", pady=(2, 0))

        # ── Edit form (hidden initially) ──────────────────
        self._edit_frame = tk.Frame(
            self, bg=Color.PRIMARY_LIGHT,
            padx=Spacing.LG, pady=Spacing.MD,
            highlightthickness=1, highlightbackground=Color.BORDER,
        )
        self._build_edit_form()

    # ─────────────────────────────────────────────────────────────────────────
    def _build_edit_form(self):
        f  = self._edit_frame
        co = self.company   # snapshot for building — live reads happen on save

        tk.Label(
            f, text="Configure Schedule",
            font=Font.LABEL_BOLD, bg=Color.PRIMARY_LIGHT, fg=Color.TEXT_PRIMARY,
        ).grid(row=0, column=0, columnspan=6, sticky="w", pady=(0, Spacing.SM))

        # Frequency selector
        tk.Label(f, text="Every:", font=Font.BODY,
                 bg=Color.PRIMARY_LIGHT, fg=Color.TEXT_SECONDARY,
                 ).grid(row=1, column=0, sticky="w", padx=(0, Spacing.XS))

        self._value_var = tk.IntVar(value=co.schedule_value)
        vcmd = (f.register(lambda s: s.isdigit() and 1 <= int(s) <= 999), "%P")
        self._value_entry = tk.Entry(
            f, textvariable=self._value_var,
            font=Font.BODY, width=5,
            bg=Color.BG_INPUT, fg=Color.TEXT_PRIMARY,
            relief="solid", bd=1,
            validate="key", validatecommand=vcmd,
        )
        self._value_entry.grid(row=1, column=1, sticky="w", padx=(0, Spacing.SM))

        self._interval_var = tk.StringVar(value=co.schedule_interval)
        interval_menu = tk.OptionMenu(
            f,
            self._interval_var,
            *self.INTERVALS,
            command=self._on_interval_change,
        )
        interval_menu.configure(
            font=Font.BODY, bg=Color.BG_INPUT, fg=Color.TEXT_PRIMARY,
            relief="solid", bd=1, width=10,
        )
        interval_menu.grid(row=1, column=2, sticky="w", padx=(0, Spacing.LG))

        # ── Daily times section ───────────────────────────────────────────────
        # Replaces the old single time entry.
        # Shows one row per configured time with a Remove button,
        # plus an "+ Add Time" button to add more.
        self._times_outer = tk.Frame(f, bg=Color.PRIMARY_LIGHT)
        self._times_outer.grid(row=1, column=3, columnspan=3, sticky="w")

        tk.Label(
            self._times_outer, text="At (HH:MM):",
            font=Font.BODY, bg=Color.PRIMARY_LIGHT, fg=Color.TEXT_SECONDARY,
        ).grid(row=0, column=0, sticky="w", padx=(0, Spacing.XS), pady=(0, 2))

        # Container for the individual time rows
        self._times_list_frame = tk.Frame(self._times_outer, bg=Color.PRIMARY_LIGHT)
        self._times_list_frame.grid(row=1, column=0, sticky="w")

        # "+ Add Time" button
        self._add_time_btn = tk.Button(
            self._times_outer, text="+ Add Time",
            font=Font.BUTTON_SM, bg=Color.BG_ROOT, fg=Color.PRIMARY,
            relief="solid", bd=1, padx=6, pady=2,
            cursor="hand2", command=lambda: self._add_time_row(),
        )
        self._add_time_btn.grid(row=2, column=0, sticky="w", pady=(4, 0))

        # Populate from existing schedule_time (comma-separated)
        self._time_entry_vars  = []
        self._time_entry_rows  = []
        existing_times = [t.strip() for t in co.schedule_time.split(",") if t.strip()]
        if not existing_times:
            existing_times = ["09:00"]
        for t in existing_times:
            self._add_time_row(t)

        # Preview label
        self._preview_lbl = tk.Label(
            f, text=self._schedule_preview(),
            font=Font.BODY_SM, bg=Color.PRIMARY_LIGHT, fg=Color.TEXT_SECONDARY,
        )
        self._preview_lbl.grid(row=2, column=0, columnspan=6, sticky="w", pady=(Spacing.SM, 0))

        # Trace interval var for preview
        self._interval_var.trace_add("write", self._update_preview)
        self._value_var.trace_add("write",    self._update_preview)

        # Save / Cancel
        btn_row = tk.Frame(f, bg=Color.PRIMARY_LIGHT)
        btn_row.grid(row=3, column=0, columnspan=6, sticky="w", pady=(Spacing.SM, 0))

        tk.Button(
            btn_row, text="✓  Save Schedule",
            font=Font.BUTTON_SM, bg=Color.PRIMARY, fg=Color.TEXT_WHITE,
            relief="flat", bd=0, padx=Spacing.MD, pady=4,
            cursor="hand2", command=self._save_schedule,
        ).pack(side="left", padx=(0, Spacing.SM))

        tk.Button(
            btn_row, text="Cancel",
            font=Font.BUTTON_SM, bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY,
            relief="solid", bd=1, padx=Spacing.MD, pady=4,
            cursor="hand2", command=self._toggle_edit,
        ).pack(side="left")

        # Set initial visibility of daily time section
        self._on_interval_change(self.company.schedule_interval)

    # ─────────────────────────────────────────────────────────────────────────
    #  Multiple-time helpers
    # ─────────────────────────────────────────────────────────────────────────
    def _add_time_row(self, time_str: str = "09:00"):
        """
        Add one time entry row (Entry + Remove button) to the daily times list.
        Appends a StringVar to self._time_entry_vars.
        """
        idx = len(self._time_entry_vars)

        row_frame = tk.Frame(self._times_list_frame, bg=Color.PRIMARY_LIGHT)
        row_frame.grid(row=idx, column=0, sticky="w", pady=(0, 3))

        var = tk.StringVar(value=time_str)
        var.trace_add("write", self._update_preview)
        self._time_entry_vars.append(var)
        self._time_entry_rows.append(row_frame)

        entry = tk.Entry(
            row_frame, textvariable=var,
            font=Font.BODY, width=8,
            bg=Color.BG_INPUT, fg=Color.TEXT_PRIMARY,
            relief="solid", bd=1,
        )
        entry.pack(side="left", padx=(0, Spacing.XS))

        # Remove button — only show if there will be at least one row left
        remove_btn = tk.Button(
            row_frame, text="✕",
            font=Font.BUTTON_SM, bg=Color.DANGER_BG, fg=Color.DANGER_FG,
            relief="flat", bd=0, padx=5, pady=2,
            cursor="hand2",
            command=lambda f=row_frame, v=var: self._remove_time_row(f, v),
        )
        remove_btn.pack(side="left")

    def _remove_time_row(self, row_frame: tk.Frame, var: tk.StringVar):
        """Remove a time entry row — keeps at least one row always."""
        if len(self._time_entry_vars) <= 1:
            messagebox.showinfo("At least one time required",
                                "You must have at least one daily sync time.")
            return
        idx = self._time_entry_rows.index(row_frame)
        self._time_entry_vars.pop(idx)
        self._time_entry_rows.pop(idx)
        row_frame.destroy()
        # Re-grid remaining rows with correct indices
        for i, r in enumerate(self._time_entry_rows):
            r.grid(row=i, column=0, sticky="w", pady=(0, 3))
        self._update_preview()

    def _get_all_times(self) -> list:
        """Return list of time strings from all time entry vars."""
        return [v.get().strip() for v in self._time_entry_vars]

    # ─────────────────────────────────────────────────────────────────────────
    #  Edit form logic
    # ─────────────────────────────────────────────────────────────────────────
    def _toggle_edit(self):
        self._editing = not self._editing
        if self._editing:
            self._edit_frame.grid(row=1, column=0, sticky="ew")
            self._edit_btn.configure(text="▲ Close")
        else:
            self._edit_frame.grid_remove()
            self._edit_btn.configure(text="✎ Edit")

    def _on_interval_change(self, val=None):
        v = self._interval_var.get()
        if v == "daily":
            self._times_outer.grid()
            self._value_entry.configure(state="disabled")
        else:
            self._times_outer.grid_remove()
            self._value_entry.configure(state="normal")
        self._update_preview()

    def _schedule_preview(self) -> str:
        try:
            interval = self._interval_var.get() if hasattr(self, "_interval_var") \
                       else self.company.schedule_interval
            value    = self._value_var.get()    if hasattr(self, "_value_var")    \
                       else self.company.schedule_value

            if interval == "minutes":
                return f"Will sync every {value} minute(s)"
            elif interval == "hourly":
                return f"Will sync every {value} hour(s)"
            elif interval == "daily":
                times = self._get_all_times() if self._time_entry_vars \
                        else [self.company.schedule_time]
                times_str = ", ".join(t for t in times if t)
                return f"Will sync every day at: {times_str}"
        except Exception:
            pass
        return ""

    def _update_preview(self, *args):
        if hasattr(self, "_preview_lbl"):
            self._preview_lbl.configure(text=self._schedule_preview())

    def _save_schedule(self):
        co = self.company
        try:
            val = int(self._value_var.get())
            if val < 1:
                raise ValueError("Value must be ≥ 1")
        except (ValueError, tk.TclError):
            messagebox.showerror("Invalid", "Please enter a valid number (≥ 1).")
            return

        interval = self._interval_var.get()

        # ── Validate and collect times for daily ─────────────────────────────
        if interval == "daily":
            import re
            raw_times = self._get_all_times()
            valid_times = []
            for t in raw_times:
                if not re.match(r"^\d{1,2}:\d{2}$", t):
                    messagebox.showerror(
                        "Invalid Time",
                        f"'{t}' is not a valid time.\nEnter each time as HH:MM, e.g. 09:00 or 01:00",
                    )
                    return
                valid_times.append(t)
            if not valid_times:
                messagebox.showerror("No Times", "Please add at least one daily sync time.")
                return
            time_s = ",".join(valid_times)   # e.g. "01:00,14:00"
        else:
            time_s = self.company.schedule_time  # unchanged for non-daily

        # ── FIX 5: Warn if interval is too short for number of scheduled companies ──
        if interval in ("minutes", "hourly"):
            AVG_SYNC_MIN = 10

            scheduled_count = sum(
                1 for c in (
                    self._state.companies.values()
                    if self._state else []
                )
                if getattr(c, 'schedule_enabled', False)
                   or c.name == co.name
            )
            scheduled_count = max(1, scheduled_count)

            if interval == "minutes":
                chosen_interval_min = max(1, val)
            else:
                chosen_interval_min = max(1, val) * 60

            estimated_round_min = scheduled_count * AVG_SYNC_MIN

            if estimated_round_min > chosen_interval_min:
                suggested_min = int(estimated_round_min * 1.2)
                if suggested_min >= 60:
                    suggested_str = f"{suggested_min // 60}h {suggested_min % 60}m" \
                                    if suggested_min % 60 else f"{suggested_min // 60} hour(s)"
                else:
                    suggested_str = f"{suggested_min} minutes"

                proceed = messagebox.askyesno(
                    "Interval May Be Too Short",
                    f"You have {scheduled_count} scheduled company/companies.\n\n"
                    f"Estimated sync time per round:  ~{estimated_round_min} min\n"
                    f"Your chosen interval:           {chosen_interval_min} min\n\n"
                    f"⚠  The round may not finish before the next one starts.\n"
                    f"   Companies at the end of the queue could be skipped.\n\n"
                    f"Recommended interval:  at least {suggested_str}\n\n"
                    f"Save anyway?",
                )
                if not proceed:
                    return

        # Apply to state
        co.schedule_interval = interval
        co.schedule_value    = val
        co.schedule_time     = time_s
        co.schedule_enabled  = True

        # Persist
        self._co_ctrl.save_one(co.name)

        # Register with APScheduler
        self._sched_ctrl.add_or_update_job(co.name)

        # Update UI
        self._toggle_enable_ui(True)
        self._update_meta()
        self._toggle_edit()

    def _toggle_enable(self):
        co = self.company
        new_state = not co.schedule_enabled
        co.schedule_enabled = new_state

        if new_state:
            self._sched_ctrl.add_or_update_job(co.name)
        else:
            self._sched_ctrl.remove_job(co.name)

        self._co_ctrl.save_one(co.name)
        self._toggle_enable_ui(new_state)
        self._update_meta()

    def _toggle_enable_ui(self, enabled: bool):
        self._status_lbl.configure(
            text=self._status_text(),
            bg=self._status_bg(),
            fg=self._status_fg(),
        )
        self._toggle_btn.configure(
            text="Disable" if enabled else "Enable",
            bg=Color.DANGER_BG if enabled else Color.SUCCESS_BG,
            fg=Color.DANGER_FG if enabled else Color.SUCCESS_FG,
        )

    def _update_meta(self):
        self._meta_lbl.configure(text=self._meta_text())

    # ─────────────────────────────────────────────────────────────────────────
    #  Public
    # ─────────────────────────────────────────────────────────────────────────
    def refresh_next_run(self):
        """
        Full UI refresh from the live CompanyState object.
        Safe to call any time — reads through the `company` property so it
        always gets the current object even after a state.companies rebuild.
        """
        co = self.company
        self._update_meta()
        self._toggle_enable_ui(co.schedule_enabled)

    # Alias for clarity when called from SchedulerPage.refresh_companies()
    refresh_from_state = refresh_next_run

    # ─────────────────────────────────────────────────────────────────────────
    #  Helpers
    # ─────────────────────────────────────────────────────────────────────────
    def _status_text(self) -> str:
        return "● Active" if self.company.schedule_enabled else "○ Disabled"

    def _status_bg(self) -> str:
        return Color.SUCCESS_BG if self.company.schedule_enabled else Color.MUTED_BG

    def _status_fg(self) -> str:
        return Color.SUCCESS_FG if self.company.schedule_enabled else Color.MUTED_FG

    @staticmethod
    def _fmt_countdown(next_run_local) -> str:
        """
        Return a human-readable countdown string to next_run_local.
        Examples:
          "in 3m 24s"    — upcoming
          "in 45s"       — less than a minute away
          "in 2h 15m"    — more than an hour away
          "overdue"      — next_run is in the past (job about to fire)
        """
        from datetime import datetime
        now     = datetime.now()
        delta   = next_run_local - now
        total_s = int(delta.total_seconds())

        if total_s <= 0:
            return "overdue"
        if total_s < 60:
            return f"in {total_s}s"
        if total_s < 3600:
            m = total_s // 60
            s = total_s % 60
            return f"in {m}m {s:02d}s"
        h = total_s // 3600
        m = (total_s % 3600) // 60
        return f"in {h}h {m}m"

    def _meta_text(self) -> str:
        co = self.company
        parts = []

        if co.schedule_enabled:
            if co.schedule_interval == "minutes":
                parts.append(f"Every {co.schedule_value} minute(s)")
            elif co.schedule_interval == "hourly":
                parts.append(f"Every {co.schedule_value} hour(s)")
            elif co.schedule_interval == "daily":
                # Show all configured times (comma-separated in DB)
                times = [t.strip() for t in co.schedule_time.split(",") if t.strip()]
                times_str = ", ".join(times) if times else co.schedule_time
                parts.append(f"Daily at {times_str}")

            # Get sync_queue_controller for round-aware status display
            sync_q = getattr(self._page_app, '_sync_queue_controller', None) \
                     if hasattr(self, '_page_app') else None

            # ── Round-aware next run label ────────────────────────────────
            from gui.controllers.company_controller import CompanyController
            round_label = CompanyController.next_run_label(
                co,
                scheduler_controller  = self._sched_ctrl,
                sync_queue_controller = sync_q,
            )

            live_statuses = ("⟳ Syncing now...", "⏳ Queued", "✓ Synced this round")
            if any(round_label.startswith(s) for s in live_statuses):
                parts.append(round_label)
            elif self._sched_ctrl:
                nrt = self._sched_ctrl.get_next_run(co.name)
                if nrt:
                    try:
                        nrt_local = nrt.astimezone().replace(tzinfo=None)
                    except Exception:
                        nrt_local = nrt.replace(tzinfo=None)
                    countdown = self._fmt_countdown(nrt_local)
                    parts.append(
                        f"Next run: {nrt_local.strftime('%d %b %Y  %H:%M')}"
                        f"  ({countdown})"
                    )
                else:
                    parts.append(f"Next run: {round_label}")
            else:
                est = CompanyController._estimate_next_run(co)
                if est != "—":
                    parts.append(f"Next run: ~{est} (est.)")
        else:
            parts.append("No schedule configured")

        # ── Last sync time ────────────────────────────────────────────────────
        if co.last_sync_time:
            ts = co.last_sync_time
            if isinstance(ts, str):
                try:
                    ts = datetime.fromisoformat(ts)
                except Exception:
                    pass
            try:
                if hasattr(ts, 'tzinfo') and ts.tzinfo is not None:
                    ts = ts.astimezone().replace(tzinfo=None)
                parts.append(f"Last sync: {ts.strftime('%d %b %Y  %H:%M')}")
            except Exception:
                pass

        # ── Tally offline indicator per company row ───────────────────────────
        if not getattr(co, 'tally_open', False):
            parts.append("⚠ Tally offline — will open automatically on next sync")

        return "  ·  ".join(parts)


# ─────────────────────────────────────────────────────────────────────────────
#  Scheduler Page
# ─────────────────────────────────────────────────────────────────────────────
class SchedulerPage(tk.Frame):

    def __init__(self, parent, state: AppState, navigate, app):
        super().__init__(parent, bg=Color.BG_ROOT)
        self.state    = state
        self.navigate = navigate
        self.app      = app

        self._rows: dict[str, ScheduleRow] = {}
        self._sched_ctrl = None    # set in on_show after app initialises scheduler
        self._co_ctrl    = None

        self.columnconfigure(0, weight=1)
        self.rowconfigure(1, weight=1)

        self._build()

        # Listen for scheduler updates from queue
        self.state.on("scheduler_updated", self._on_scheduler_updated)

    # ─────────────────────────────────────────────────────────────────────────
    def _build(self):
        self.columnconfigure(0, weight=1)
        self.rowconfigure(2, weight=1)   # row 2 = list (row 1 = queue strip)

        # ── Toolbar ───────────────────────────────────────
        toolbar = tk.Frame(self, bg=Color.BG_ROOT, pady=Spacing.MD)
        toolbar.grid(row=0, column=0, sticky="ew", padx=Spacing.XL)
        toolbar.columnconfigure(0, weight=1)

        self._sched_status_lbl = tk.Label(
            toolbar,
            text="⏰  Scheduler: Initialising...",
            font=Font.BODY,
            bg=Color.BG_ROOT,
            fg=Color.TEXT_SECONDARY,
        )
        self._sched_status_lbl.grid(row=0, column=0, sticky="w")

        right = tk.Frame(toolbar, bg=Color.BG_ROOT)
        right.grid(row=0, column=1)

        tk.Button(
            right, text="⟳  Refresh",
            font=Font.BUTTON_SM, bg=Color.BG_CARD, fg=Color.TEXT_PRIMARY,
            relief="solid", bd=1, padx=Spacing.SM, pady=3,
            cursor="hand2", command=self._refresh_next_runs,
        ).pack(side="left", padx=(0, Spacing.SM))

        tk.Button(
            right, text="✖  Disable All",
            font=Font.BUTTON_SM, bg=Color.DANGER_BG, fg=Color.DANGER_FG,
            relief="flat", bd=0, padx=Spacing.SM, pady=3,
            cursor="hand2", command=self._disable_all,
        ).pack(side="left")

        # ── Phase 2: Queue Status Strip ───────────────────
        self._queue_strip = tk.Frame(
            self, bg=Color.PRIMARY_LIGHT,
            highlightthickness=1, highlightbackground=Color.BORDER,
        )
        self._queue_strip_lbl = tk.Label(
            self._queue_strip,
            text="",
            font=Font.BODY_SM,
            bg=Color.PRIMARY_LIGHT,
            fg=Color.TEXT_PRIMARY,
            anchor="w",
            padx=Spacing.LG,
            pady=Spacing.SM,
        )
        self._queue_strip_lbl.pack(fill="x")
        self._queue_strip_visible = False

        # ── Scrollable list ───────────────────────────────
        container = tk.Frame(
            self, bg=Color.BG_CARD,
            highlightthickness=1, highlightbackground=Color.BORDER,
        )
        container.grid(
            row=2, column=0, sticky="nsew",
            padx=Spacing.XL, pady=(0, Spacing.MD),
        )
        container.rowconfigure(1, weight=1)
        container.columnconfigure(0, weight=1)

        # Column headers
        hdr = tk.Frame(container, bg=Color.BG_TABLE_HEADER)
        hdr.grid(row=0, column=0, columnspan=2, sticky="ew")
        for col, (text, w) in enumerate([
            ("Company",  40),
            ("Schedule", 20),
            ("Actions",  16),
        ]):
            tk.Label(
                hdr, text=text, font=Font.BODY_SM_BOLD,
                bg=Color.BG_TABLE_HEADER, fg=Color.TEXT_SECONDARY,
                anchor="w", padx=Spacing.SM, pady=Spacing.SM, width=w,
            ).grid(row=0, column=col, sticky="ew")
        tk.Frame(container, bg=Color.BORDER, height=1).grid(
            row=0, column=0, columnspan=2, sticky="sew",
        )

        canvas = tk.Canvas(container, bg=Color.BG_CARD, highlightthickness=0, bd=0)
        canvas.grid(row=1, column=0, sticky="nsew")
        vsb = tk.Scrollbar(container, orient="vertical", command=canvas.yview)
        vsb.grid(row=1, column=1, sticky="ns")
        canvas.configure(yscrollcommand=vsb.set)

        self._list_frame = tk.Frame(canvas, bg=Color.BG_CARD)
        self._list_frame.columnconfigure(0, weight=1)
        cw = canvas.create_window((0, 0), window=self._list_frame, anchor="nw")
        self._list_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        canvas.bind("<Configure>", lambda e: canvas.itemconfig(cw, width=e.width))
        canvas.bind_all(
            "<MouseWheel>",
            lambda e: canvas.yview_scroll(int(-1 * (e.delta / 120)), "units")
        )

        # ── No scheduler warning ──────────────────────────
        self._no_sched_lbl = tk.Label(
            self._list_frame,
            text="APScheduler not installed.\n\nRun:  pip install apscheduler\nthen restart the app.",
            font=Font.BODY, bg=Color.BG_CARD, fg=Color.WARNING_FG,
            justify="center", pady=40,
        )

    # ─────────────────────────────────────────────────────────────────────────
    #  Render rows
    # ─────────────────────────────────────────────────────────────────────────
    def _render_rows(self):
        for w in self._list_frame.winfo_children():
            w.destroy()
        self._rows.clear()

        try:
            from apscheduler.schedulers.background import BackgroundScheduler
            has_apscheduler = True
        except ImportError:
            has_apscheduler = False

        if not has_apscheduler:
            self._no_sched_lbl = tk.Label(
                self._list_frame,
                text="APScheduler not installed.\n\nRun:  pip install apscheduler\nthen restart the app.",
                font=Font.BODY, bg=Color.BG_CARD, fg=Color.WARNING_FG,
                justify="center", pady=40,
            )
            self._no_sched_lbl.pack(fill="both", expand=True)
            return

        companies = sorted(
            [
                co for co in self.state.companies.values()
                if co.status != CompanyStatus.NOT_CONFIGURED
                or getattr(co, 'schedule_enabled', False)
                or bool(getattr(co, 'guid', ''))
            ],
            key=lambda c: (0 if c.schedule_enabled else 1, c.name.lower())
        )

        if not companies:
            tk.Label(
                self._list_frame,
                text=(
                    "No companies loaded.\n\n"
                    "Go to Companies page and click ⟳ Refresh.\n"
                    "If Tally is offline, companies still appear here\n"
                    "once they have been configured at least once."
                ),
                font=Font.BODY, bg=Color.BG_CARD, fg=Color.TEXT_MUTED,
                pady=40, justify="center",
            ).pack(fill="both", expand=True)
            return

        for i, co in enumerate(companies):
            row = ScheduleRow(
                parent      = self._list_frame,
                company     = co,
                controller  = self._sched_ctrl,
                co_ctrl     = self._co_ctrl,
                on_run_now  = self._on_run_now,
                state       = self.state,
                app         = self.app,
            )
            bg = Color.BG_TABLE_ODD if i % 2 == 0 else Color.BG_TABLE_EVEN
            row.configure(bg=bg)
            row.pack(fill="x")
            self._rows[co.name] = row
            tk.Frame(self._list_frame, bg=Color.BORDER_LIGHT, height=1).pack(fill="x")

    # ─────────────────────────────────────────────────────────────────────────
    #  Toolbar actions
    # ─────────────────────────────────────────────────────────────────────────
    def _refresh_next_runs(self):
        """Manually refresh all next-run labels (toolbar button)."""
        for row in self._rows.values():
            row.refresh_next_run()

    def refresh_companies(self):
        """
        Called by app.py whenever companies are reloaded (startup or Refresh).
        Rebuilds the row list so new CompanyState objects are picked up.
        Also re-syncs APScheduler jobs for any enabled schedules.
        Must be called on the main (GUI) thread.
        """
        self._render_rows()
        self._update_scheduler_status()

        if self._sched_ctrl and self._sched_ctrl.is_running():
            for name, co in self.state.companies.items():
                if co.schedule_enabled and co.status != CompanyStatus.NOT_CONFIGURED:
                    try:
                        self._sched_ctrl.add_or_update_job(name)
                    except Exception:
                        pass

    def _disable_all(self):
        if not messagebox.askyesno(
            "Disable All Schedules",
            "Disable all scheduled syncs?\n\nYou can re-enable them individually.",
        ):
            return
        for name, co in self.state.companies.items():
            if co.schedule_enabled and co.status != CompanyStatus.NOT_CONFIGURED:
                co.schedule_enabled = False
                if self._sched_ctrl:
                    self._sched_ctrl.remove_job(name)
        if self._co_ctrl:
            self._co_ctrl.save_scheduler_config()
        self._render_rows()

    def _on_run_now(self, company_name: str):
        """
        Trigger an immediate manual sync for this company via SyncQueueController.
        Phase 2 Fix 9: Show queue position and estimated wait time in confirmation.
        """
        co = self.state.get_company(company_name)
        if not co or co.status == CompanyStatus.NOT_CONFIGURED:
            messagebox.showwarning(
                "Not Configured",
                f"'{company_name}' has not been configured yet.\n\n"
                "Go to the Companies page and click ⚙ Configure first.",
            )
            return

        sync_q = getattr(self.app, '_sync_queue_controller', None)
        if sync_q is not None:

            current = sync_q.current_company
            waiting = list(sync_q.queued_companies)

            if company_name == current:
                messagebox.showinfo(
                    "Already Syncing",
                    f"'{company_name}' is currently syncing right now.\n\n"
                    "Check the Logs page for progress.",
                )
                return

            if company_name in waiting:
                pos       = waiting.index(company_name) + 1
                total     = len(waiting)
                ahead     = pos - 1
                AVG_SYNC_MIN = 10
                wait_min  = ahead * AVG_SYNC_MIN
                if current:
                    wait_min += AVG_SYNC_MIN
                messagebox.showinfo(
                    "Already Queued",
                    f"'{company_name}' is already in the sync queue.\n\n"
                    f"Position:       {pos} of {total}\n"
                    f"Estimated wait: ~{wait_min} min\n\n"
                    "Check the Logs page for progress.",
                )
                return

            if sync_q.round_active and company_name in sync_q.round_companies:
                messagebox.showinfo(
                    "Already Synced This Round",
                    f"'{company_name}' has already synced in the current round.\n\n"
                    f"The round is still in progress for other companies.\n"
                    f"It will be available again when the round completes.\n\n"
                    "Check the Logs page to see which companies are running.",
                )
                return

            sync_q.enqueue(company_name)

            new_waiting  = list(sync_q.queued_companies)
            new_pos      = len(new_waiting)
            total_ahead  = new_pos - 1
            AVG_SYNC_MIN = 10
            wait_min     = total_ahead * AVG_SYNC_MIN
            if current:
                wait_min += AVG_SYNC_MIN

            wait_str = "starting shortly" if wait_min == 0 else f"~{wait_min} min wait"

            messagebox.showinfo(
                "Added to Queue",
                f"'{company_name}' has been added to the sync queue.\n\n"
                f"Queue position:  {new_pos}\n"
                f"Estimated wait:  {wait_str}\n\n"
                "Tally will open automatically when it's this company's turn.\n"
                "Check the Logs page for progress.",
            )
            return

        # Fallback: navigate to manual sync page
        if self.state.sync_active:
            messagebox.showwarning("Sync Running", "A sync is already in progress.")
            return
        self.state.selected_companies = [company_name]
        self.navigate("sync")

    # ─────────────────────────────────────────────────────────────────────────
    #  Init controllers (lazy — wait for app to create scheduler)
    # ─────────────────────────────────────────────────────────────────────────
    def _init_controllers(self):
        """
        Wire up scheduler and company controllers.

        Phase 2: app.py already starts SchedulerController in _startup_worker.
        We reuse that instance instead of creating a duplicate — a second
        APScheduler instance on the same DB jobstore causes job duplication.
        """
        from gui.controllers.company_controller import CompanyController

        self._co_ctrl = CompanyController(self.state)

        existing = getattr(self.app, '_scheduler_controller', None)
        if existing is not None:
            self._sched_ctrl = existing
            return

        self._sched_ctrl = None

    def _update_scheduler_status(self):
        """
        FIX 2: Always re-wire _sched_ctrl before checking status.
        """
        if self._sched_ctrl is None:
            existing = getattr(self.app, '_scheduler_controller', None)
            if existing is not None:
                self._sched_ctrl = existing
                for row in self._rows.values():
                    row._sched_ctrl = self._sched_ctrl

        if self._sched_ctrl and self._sched_ctrl.is_running():
            enabled = sum(1 for c in self.state.companies.values() if c.schedule_enabled)
            tally_ok = self.state.tally.connected
            tally_txt = "  ·  Tally: ● Online" if tally_ok else "  ·  Tally: ○ Offline"
            self._sched_status_lbl.configure(
                text=(
                    f"⏰  Scheduler: ● Running  |  {enabled} active job(s)"
                    f"{tally_txt}"
                ),
                fg=Color.SUCCESS,
            )
        elif self._sched_ctrl is not None:
            self._sched_status_lbl.configure(
                text="⏰  Scheduler: ○ Starting...",
                fg=Color.WARNING_FG,
            )
        else:
            self._sched_status_lbl.configure(
                text="⏰  Scheduler: ○ Not running",
                fg=Color.MUTED,
            )

    def on_scheduler_ready(self):
        """
        Called by app.py via 'scheduler_ready' queue event.
        Fires after BOTH SchedulerController and SyncQueueController are started.
        """
        self._sched_ctrl = getattr(self.app, '_scheduler_controller', None)
        if self._co_ctrl is None:
            from gui.controllers.company_controller import CompanyController
            self._co_ctrl = CompanyController(self.state)

        self._update_scheduler_status()
        self._render_rows()
        self._start_status_ticker()

        from logging_config import logger
        logger.info("[SchedulerPage] Scheduler ready — page refreshed ✓")

    def _start_status_ticker(self):
        """Periodic status bar refresh every 10 seconds."""
        if getattr(self, "_status_ticker_active", False):
            return
        self._status_ticker_active = True
        self._tick_status()

    def _tick_status(self):
        if not getattr(self, "_status_ticker_active", False):
            return
        try:
            self._update_scheduler_status()
        except Exception:
            pass
        self.after(10_000, self._tick_status)

    # ─────────────────────────────────────────────────────────────────────────
    #  State event callbacks
    # ─────────────────────────────────────────────────────────────────────────
    def _on_scheduler_updated(self, **kwargs):
        def _do():
            self._update_scheduler_status()
            self._refresh_next_runs()
        self.after(0, _do)

    def refresh_queue_status(self):
        """
        Called by app.py on every 'queue_updated' event.
        Updates the live queue strip at top of page.
        """
        self.after(0, self._update_queue_strip)

    def _update_queue_strip(self):
        """Rebuild the queue status strip text and show/hide it."""
        sync_q = getattr(self.app, '_sync_queue_controller', None)
        if sync_q is None:
            self._hide_queue_strip()
            return

        current  = sync_q.current_company
        waiting  = list(sync_q.queued_companies)

        if not current and not waiting:
            if self._queue_strip_visible:
                done_companies = getattr(self, '_last_done_companies', [])
                if done_companies:
                    done_str = ", ".join(done_companies[-6:])
                    self._queue_strip_lbl.configure(
                        text=f"✓  All syncs complete  ·  Done: {done_str}",
                        fg=Color.SUCCESS,
                    )
                    self.after(5_000, self._hide_queue_strip)
            return

        parts = []

        if current:
            parts.append(f"● Syncing: {current}")

            round_cos = sync_q.round_companies
            done = [
                n for n in round_cos
                if n != current and n not in waiting
            ]
            self._last_done_companies = done
        else:
            done = []

        if waiting:
            waiting_str = ", ".join(waiting)
            parts.append(f"⏳ Waiting ({len(waiting)}): {waiting_str}")

        if done:
            done_str = ", ".join(done[-4:])
            if len(done) > 4:
                done_str = f"...{done_str}"
            parts.append(f"✓ Done ({len(done)}): {done_str}")

        strip_text = "   |   ".join(parts)
        self._queue_strip_lbl.configure(
            text=strip_text,
            fg=Color.TEXT_PRIMARY,
        )
        self._show_queue_strip()

    def _show_queue_strip(self):
        if not self._queue_strip_visible:
            self._queue_strip.grid(
                row=1, column=0, sticky="ew",
                padx=Spacing.XL, pady=(0, Spacing.XS),
            )
            self._queue_strip_visible = True

    def _hide_queue_strip(self):
        if self._queue_strip_visible:
            self._queue_strip.grid_remove()
            self._queue_strip_visible = False

    # ─────────────────────────────────────────────────────────────────────────
    #  Live next-run ticker
    # ─────────────────────────────────────────────────────────────────────────
    def _start_next_run_ticker(self):
        """Ticks every 1 second to keep the countdown live."""
        if getattr(self, "_ticker_active", False):
            return
        self._ticker_active = True
        self._tick_next_runs()

    def _tick_next_runs(self):
        """Refresh all row meta labels every second for live countdown."""
        if not self._ticker_active:
            return
        for row in self._rows.values():
            try:
                row.refresh_next_run()
            except Exception:
                pass
        self.after(1_000, self._tick_next_runs)

    # ─────────────────────────────────────────────────────────────────────────
    #  Lifecycle
    # ─────────────────────────────────────────────────────────────────────────
    def on_show(self):
        """Called every time user navigates to the Scheduler page."""
        self._init_controllers()

        if self._sched_ctrl is None:
            existing = getattr(self.app, '_scheduler_controller', None)
            if existing is not None:
                self._sched_ctrl = existing
                for row in self._rows.values():
                    row._sched_ctrl = self._sched_ctrl

        self._update_scheduler_status()

        if not self._rows:
            self._render_rows()
        else:
            for row in self._rows.values():
                try:
                    row.refresh_next_run()
                except Exception:
                    pass

        self._start_next_run_ticker()
        self._start_status_ticker()
