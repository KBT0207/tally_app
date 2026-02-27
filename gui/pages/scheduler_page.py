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
│  Every: [1] [Day ▾]  At: [09:00]  Next run: —                  │
│  Vouchers: Sales, Purchase           [Edit]  [Run Now]  [Enable]│
└─────────────────────────────────────────────────────────────────┘

Edit opens an inline form that expands below the row.
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
        state:      AppState = None,   # NEW — live state reference
        **kwargs,
    ):
        super().__init__(parent, bg=Color.BG_CARD, **kwargs)
        self._name       = company.name   # key into state.companies
        self._state      = state          # live AppState — read company from here
        self._sched_ctrl = controller
        self._co_ctrl    = co_ctrl
        self._on_run_now = on_run_now
        self._editing    = False
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

        # Daily time picker (shown only for daily)
        self._time_lbl = tk.Label(f, text="At (HH:MM):", font=Font.BODY,
                                  bg=Color.PRIMARY_LIGHT, fg=Color.TEXT_SECONDARY)
        self._time_var = tk.StringVar(value=co.schedule_time)
        self._time_entry = tk.Entry(
            f, textvariable=self._time_var,
            font=Font.BODY, width=8,
            bg=Color.BG_INPUT, fg=Color.TEXT_PRIMARY,
            relief="solid", bd=1,
        )

        self._time_lbl.grid(row=1, column=3, sticky="w", padx=(0, Spacing.XS))
        self._time_entry.grid(row=1, column=4, sticky="w")

        # Preview label
        self._preview_lbl = tk.Label(
            f, text=self._schedule_preview(),
            font=Font.BODY_SM, bg=Color.PRIMARY_LIGHT, fg=Color.TEXT_SECONDARY,
        )
        self._preview_lbl.grid(row=2, column=0, columnspan=5, sticky="w", pady=(Spacing.SM, 0))

        # Trace vars to update preview live
        self._value_var.trace_add("write",    self._update_preview)
        self._interval_var.trace_add("write", self._update_preview)
        self._time_var.trace_add("write",     self._update_preview)

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

        # Set initial visibility of daily time fields
        self._on_interval_change(self.company.schedule_interval)

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
            self._time_lbl.grid()
            self._time_entry.grid()
            self._value_entry.configure(state="disabled")
        else:
            self._time_lbl.grid_remove()
            self._time_entry.grid_remove()
            self._value_entry.configure(state="normal")
        self._update_preview()

    def _schedule_preview(self) -> str:
        try:
            interval = self._interval_var.get() if hasattr(self, "_interval_var") \
                       else self.company.schedule_interval
            value    = self._value_var.get()    if hasattr(self, "_value_var")    \
                       else self.company.schedule_value
            time_s   = self._time_var.get()     if hasattr(self, "_time_var")     \
                       else self.company.schedule_time

            if interval == "minutes":
                return f"Will sync every {value} minute(s)"
            elif interval == "hourly":
                return f"Will sync every {value} hour(s)"
            elif interval == "daily":
                return f"Will sync every day at {time_s}"
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
        time_s   = self._time_var.get().strip()

        if interval == "daily":
            # Validate HH:MM
            import re
            if not re.match(r"^\d{1,2}:\d{2}$", time_s):
                messagebox.showerror("Invalid Time", "Enter time as HH:MM, e.g. 09:00")
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
        # Update meta text (schedule + next run)
        self._update_meta()
        # Update status badge and enable/disable button to match live state
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

    def _meta_text(self) -> str:
        co = self.company
        parts = []

        if co.schedule_enabled:
            if co.schedule_interval == "minutes":
                parts.append(f"Every {co.schedule_value} minute(s)")
            elif co.schedule_interval == "hourly":
                parts.append(f"Every {co.schedule_value} hour(s)")
            elif co.schedule_interval == "daily":
                parts.append(f"Daily at {co.schedule_time}")

            # Next run from APScheduler
            if self._sched_ctrl:
                nrt = self._sched_ctrl.get_next_run(co.name)
                if nrt:
                    parts.append(f"Next run: {nrt.strftime('%d %b %Y  %H:%M')}")
                else:
                    from gui.controllers.company_controller import CompanyController
                    parts.append(f"Next run: {CompanyController.next_run_label(co)}")
        else:
            parts.append("No schedule configured")

        if co.last_sync_time:
            ts = co.last_sync_time
            if isinstance(ts, str):
                try:
                    ts = datetime.fromisoformat(ts)
                except Exception:
                    pass
            try:
                parts.append(f"Last sync: {ts.strftime('%d %b %Y  %H:%M')}")
            except Exception:
                pass

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

        # ── Scrollable list ───────────────────────────────
        container = tk.Frame(
            self, bg=Color.BG_CARD,
            highlightthickness=1, highlightbackground=Color.BORDER,
        )
        container.grid(
            row=1, column=0, sticky="nsew",
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

        # Only show CONFIGURED companies — unconfigured ones cannot be scheduled
        companies = sorted(
            [c for c in self.state.companies.values()
             if c.status != CompanyStatus.NOT_CONFIGURED],
            key=lambda c: (0 if c.schedule_enabled else 1, c.name.lower())
        )

        if not companies:
            total = len(self.state.companies)
            if total == 0:
                msg = "No companies loaded.\nGo to Companies page and click ⟳ Refresh."
            else:
                msg = (
                    f"{total} company/companies found but none are configured yet.\n\n"
                    "Go to the Companies page, click ⚙ Configure on each company,\n"
                    "then return here to set up a schedule."
                )
            tk.Label(
                self._list_frame, text=msg,
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
                state       = self.state,       # ← pass live state so row never goes stale
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
        # Rebuild all rows — each ScheduleRow now holds a live state reference
        self._render_rows()
        self._update_scheduler_status()

        # Re-register APScheduler jobs for companies that have scheduling enabled
        # (needed if scheduler was already running before refresh)
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
        """Trigger an immediate manual sync for this company."""
        co = self.state.get_company(company_name)
        if not co or co.status == CompanyStatus.NOT_CONFIGURED:
            messagebox.showwarning(
                "Not Configured",
                f"'{company_name}' has not been configured yet.\n\n"
                "Go to the Companies page and click ⚙ Configure first.",
            )
            return
        if self.state.sync_active:
            messagebox.showwarning("Sync Running", "A sync is already in progress.")
            return
        self.state.selected_companies = [company_name]
        self.navigate("sync")

    # ─────────────────────────────────────────────────────────────────────────
    #  Init controllers (lazy — wait for app to create scheduler)
    # ─────────────────────────────────────────────────────────────────────────
    def _init_controllers(self):
        if self._sched_ctrl is not None:
            return  # already done

        from gui.controllers.company_controller   import CompanyController
        from gui.controllers.scheduler_controller import SchedulerController

        self._co_ctrl    = CompanyController(self.state)
        self._sched_ctrl = SchedulerController(self.state, self.app._q)

        # Load saved config → state
        self._co_ctrl.load_scheduler_config()

        # Start APScheduler
        self._sched_ctrl.start()

        # Give app a reference for clean shutdown
        self.app._scheduler_controller = self._sched_ctrl

    def _update_scheduler_status(self):
        if self._sched_ctrl and self._sched_ctrl.is_running():
            jobs = self._sched_ctrl.get_all_jobs()
            enabled = sum(1 for c in self.state.companies.values() if c.schedule_enabled)
            self._sched_status_lbl.configure(
                text=f"⏰  Scheduler: ● Running  |  {enabled} active job(s)",
                fg=Color.SUCCESS,
            )
        else:
            self._sched_status_lbl.configure(
                text="⏰  Scheduler: ○ Not running",
                fg=Color.MUTED,
            )

    # ─────────────────────────────────────────────────────────────────────────
    #  State event callbacks
    # ─────────────────────────────────────────────────────────────────────────
    def _on_scheduler_updated(self, **kwargs):
        def _do():
            self._update_scheduler_status()
            self._refresh_next_runs()
        self.after(0, _do)

    # ─────────────────────────────────────────────────────────────────────────
    #  Live next-run ticker — refreshes all rows every 30 s automatically
    # ─────────────────────────────────────────────────────────────────────────
    def _start_next_run_ticker(self):
        """
        Called once on first on_show().  Ticks every 30 s to keep "Next run"
        labels accurate without the user having to click ⟳ Refresh.
        """
        if getattr(self, "_ticker_active", False):
            return   # already running
        self._ticker_active = True
        self._tick_next_runs()

    def _tick_next_runs(self):
        """Scheduled callback — refreshes all row meta labels."""
        if not self._ticker_active:
            return
        for row in self._rows.values():
            try:
                row.refresh_next_run()
            except Exception:
                pass
        # Re-schedule every 30 seconds
        self.after(30_000, self._tick_next_runs)

    # ─────────────────────────────────────────────────────────────────────────
    #  Lifecycle
    # ─────────────────────────────────────────────────────────────────────────
    def on_show(self):
        self._init_controllers()
        self._update_scheduler_status()
        self._render_rows()
        self._start_next_run_ticker()   # idempotent — safe to call each time