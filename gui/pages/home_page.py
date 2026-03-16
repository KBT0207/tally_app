"""
gui/pages/home_page.py
=======================
Home page — full company list.

Phase 2 fixes:
  - bind_all("<MouseWheel>") replaced with canvas-scoped binding
    (old code leaked scroll events to all pages)
  - _render_cards() now does IN-PLACE update when possible
    (no more destroy+recreate flicker during active sync)
  - Cards only fully rebuilt when company list changes (add/remove)
  - Status + progress updates go directly to existing card widgets
"""

import threading
import tkinter as tk
from tkinter import messagebox

from gui.state  import AppState, CompanyState, CompanyStatus
from gui.styles import Color, Font, Spacing
from gui.components.company_card import CompanyCard


class HomePage(tk.Frame):

    def __init__(self, parent, state: AppState, navigate, app):
        super().__init__(parent, bg=Color.BG_ROOT)
        self.state    = state
        self.navigate = navigate
        self.app      = app

        self._cards: dict[str, CompanyCard] = {}
        self._filter_var = tk.StringVar()
        self._filter_var.trace_add("write", self._on_filter_change)

        # Track last rendered company set to detect add/remove
        self._last_rendered_keys: list[str] = []

        self._build()

        # Register state event listeners
        self.state.on("company_updated",  self._on_company_updated)
        self.state.on("company_progress", self._on_company_progress)
        self.state.on("sync_finished",    self._on_sync_finished)

    # ─────────────────────────────────────────────────────────────────────────
    #  Layout
    # ─────────────────────────────────────────────────────────────────────────
    def _build(self):
        self.columnconfigure(0, weight=1)
        self.rowconfigure(1, weight=1)
        self._build_toolbar()
        self._build_list_area()
        self._build_action_bar()

    def _build_toolbar(self):
        bar = tk.Frame(self, bg=Color.BG_ROOT, pady=Spacing.MD)
        bar.grid(row=0, column=0, sticky="ew", padx=Spacing.XL)
        bar.columnconfigure(1, weight=1)

        self._summary_lbl = tk.Label(
            bar, text="Loading companies...",
            font=Font.BODY, bg=Color.BG_ROOT, fg=Color.TEXT_SECONDARY,
        )
        self._summary_lbl.grid(row=0, column=0, sticky="w")

        right = tk.Frame(bar, bg=Color.BG_ROOT)
        right.grid(row=0, column=1, sticky="e")

        # Search
        tk.Label(right, text="🔍", font=Font.BODY,
                 bg=Color.BG_ROOT, fg=Color.TEXT_MUTED).pack(side="left", padx=(0, 4))

        tk.Entry(
            right, textvariable=self._filter_var,
            font=Font.BODY, bg=Color.BG_INPUT, fg=Color.TEXT_PRIMARY,
            relief="solid", bd=1, width=22,
        ).pack(side="left")

        # Filter toggle
        self._show_all_var = tk.BooleanVar(value=True)
        tk.Checkbutton(
            right, text="Show unconfigured",
            variable=self._show_all_var,
            font=Font.BODY_SM,
            bg=Color.BG_ROOT, activebackground=Color.BG_ROOT,
            fg=Color.TEXT_SECONDARY,
            command=lambda: self._render_cards(self._filter_var.get().strip()),
        ).pack(side="left", padx=(Spacing.MD, 2))

        # Select All / Clear
        for label, cmd in [("Select All", self._select_all), ("Clear", self._deselect_all)]:
            tk.Button(
                right, text=label, font=Font.BUTTON_SM,
                bg=Color.BG_CARD, fg=Color.TEXT_PRIMARY,
                relief="solid", bd=1, padx=Spacing.SM, pady=2,
                cursor="hand2", command=cmd,
            ).pack(side="left", padx=(Spacing.MD if label == "Select All" else 2, 2))

        # Refresh
        self._refresh_btn = tk.Button(
            right, text="⟳  Refresh", font=Font.BUTTON_SM,
            bg=Color.PRIMARY, fg=Color.TEXT_WHITE,
            relief="flat", bd=0, padx=Spacing.MD, pady=4,
            cursor="hand2", command=self._on_refresh,
        )
        self._refresh_btn.pack(side="left", padx=(Spacing.MD, 0))


    def _build_list_area(self):
        container = tk.Frame(
            self, bg=Color.BG_CARD, relief="flat",
            highlightthickness=1, highlightbackground=Color.BORDER,
        )
        container.grid(
            row=1, column=0, sticky="nsew",
            padx=Spacing.XL, pady=(0, Spacing.MD),
        )
        container.rowconfigure(1, weight=1)
        container.columnconfigure(0, weight=1)

        # Column header
        hdr = tk.Frame(container, bg=Color.BG_TABLE_HEADER)
        hdr.grid(row=0, column=0, columnspan=2, sticky="ew")

        for col, (text, w) in enumerate([
            ("",               3),
            ("Company Name",  34),
            ("Status",        14),
            ("Sync Progress", 14),
            ("Actions",       10),
        ]):
            tk.Label(
                hdr, text=text, font=Font.BODY_SM_BOLD,
                bg=Color.BG_TABLE_HEADER, fg=Color.TEXT_SECONDARY,
                anchor="w", padx=Spacing.SM, pady=Spacing.SM, width=w,
            ).grid(row=0, column=col, sticky="ew")

        tk.Frame(container, bg=Color.BORDER, height=1).grid(
            row=0, column=0, columnspan=2, sticky="sew",
        )

        # Scrollable canvas
        self._canvas = tk.Canvas(
            container, bg=Color.BG_CARD, highlightthickness=0, bd=0,
        )
        self._canvas.grid(row=1, column=0, sticky="nsew")

        vsb = tk.Scrollbar(container, orient="vertical", command=self._canvas.yview)
        vsb.grid(row=1, column=1, sticky="ns")
        self._canvas.configure(yscrollcommand=vsb.set)

        self._list_frame = tk.Frame(self._canvas, bg=Color.BG_CARD)
        self._list_frame.columnconfigure(0, weight=1)

        cw = self._canvas.create_window((0, 0), window=self._list_frame, anchor="nw")

        self._list_frame.bind(
            "<Configure>",
            lambda e: self._canvas.configure(scrollregion=self._canvas.bbox("all"))
        )
        self._canvas.bind(
            "<Configure>",
            lambda e: self._canvas.itemconfig(cw, width=e.width)
        )

        # ── Phase 2 fix: scope mousewheel ONLY to the canvas ──────────────────
        # Old: bind_all leaked scroll to every page in the app
        # New: bind enters/leaves canvas — only active when mouse is over the list
        self._canvas.bind("<Enter>", self._on_canvas_enter)
        self._canvas.bind("<Leave>", self._on_canvas_leave)

    def _on_canvas_enter(self, e):
        """Enable mousewheel scrolling when mouse enters the company list."""
        self._canvas.bind_all("<MouseWheel>",  self._on_mousewheel)
        self._canvas.bind_all("<Button-4>",    self._on_mousewheel_linux)
        self._canvas.bind_all("<Button-5>",    self._on_mousewheel_linux)

    def _on_canvas_leave(self, e):
        """Disable mousewheel scrolling when mouse leaves the company list."""
        self._canvas.unbind_all("<MouseWheel>")
        self._canvas.unbind_all("<Button-4>")
        self._canvas.unbind_all("<Button-5>")

    def _on_mousewheel(self, e):
        """Windows / Mac scroll handler."""
        self._canvas.yview_scroll(int(-1 * (e.delta / 120)), "units")

    def _on_mousewheel_linux(self, e):
        """Linux scroll handler (Button-4 = up, Button-5 = down)."""
        if e.num == 4:
            self._canvas.yview_scroll(-1, "units")
        elif e.num == 5:
            self._canvas.yview_scroll(1, "units")

    def _build_action_bar(self):
        bar = tk.Frame(
            self, bg=Color.BG_HEADER,
            highlightthickness=1, highlightbackground=Color.BORDER,
            pady=Spacing.MD, padx=Spacing.XL,
        )
        bar.grid(row=2, column=0, sticky="ew")
        bar.columnconfigure(0, weight=1)

        self._selection_lbl = tk.Label(
            bar, text="No companies selected",
            font=Font.BODY, bg=Color.BG_HEADER, fg=Color.TEXT_SECONDARY,
        )
        self._selection_lbl.grid(row=0, column=0, sticky="w")

        btns = tk.Frame(bar, bg=Color.BG_HEADER)
        btns.grid(row=0, column=1)

        self._sync_sel_btn = tk.Button(
            btns, text="▶  Sync Selected",
            font=Font.BUTTON, bg=Color.PRIMARY, fg=Color.TEXT_WHITE,
            relief="flat", bd=0, padx=Spacing.LG, pady=Spacing.SM,
            cursor="hand2", state="disabled",
            command=self._on_sync_selected,
        )
        self._sync_sel_btn.pack(side="left", padx=(0, Spacing.SM))

        self._sched_sel_btn = tk.Button(
            btns, text="⏰  Schedule Selected",
            font=Font.BUTTON, bg=Color.INFO_BG, fg=Color.INFO_FG,
            relief="flat", bd=0, padx=Spacing.LG, pady=Spacing.SM,
            cursor="hand2", state="disabled",
            command=self._on_schedule_selected,
        )
        self._sched_sel_btn.pack(side="left")

    # ─────────────────────────────────────────────────────────────────────────
    #  Render cards — Phase 2: smart update vs full rebuild
    # ─────────────────────────────────────────────────────────────────────────
    def refresh_companies(self):
        """Called by app.py after DB + Tally load completes."""
        # force_rebuild=True so button layout (Configure vs Sync/Edit/Schedule)
        # is always rebuilt correctly after a DB reload
        self._render_cards(force_rebuild=True)
        self._update_summary()

    def _render_cards(self, filter_text: str = "", force_rebuild: bool = False):
        """
        Smart render:
        - If the set of visible companies has NOT changed → update cards in place
        - If companies were added or removed → full rebuild
        This prevents flicker/destruction of cards during active sync.
        """
        companies = self._get_filtered_companies(filter_text)
        new_keys  = [c.name for c in companies]

        if new_keys == self._last_rendered_keys and not force_rebuild:
            # ── In-place update: only touch status/badge, not widgets ─────────
            for co in companies:
                if co.name in self._cards:
                    self._cards[co.name].update_status(co.status)
            return

        # ── Full rebuild: company list changed ────────────────────────────────
        self._last_rendered_keys = new_keys

        for w in self._list_frame.winfo_children():
            w.destroy()
        self._cards.clear()

        if not companies:
            self._render_empty_state(filter_text)
            return

        # Sort: configured first, then alphabetical within each group
        companies.sort(key=lambda c: (
            0 if c.status != CompanyStatus.NOT_CONFIGURED else 1,
            c.name.lower(),
        ))

        shown_configured   = False
        shown_unconfigured = False

        for i, co in enumerate(companies):
            is_configured = (co.status != CompanyStatus.NOT_CONFIGURED)

            if is_configured and not shown_configured:
                self._add_section_header("✓  Configured Companies", Color.SUCCESS_FG)
                shown_configured = True
            elif not is_configured and not shown_unconfigured:
                if shown_configured:
                    tk.Frame(self._list_frame, bg=Color.BORDER, height=1).pack(
                        fill="x", pady=(4, 0)
                    )
                self._add_section_header(
                    "○  Not Yet Configured  —  open in Tally, not saved to DB",
                    Color.WARNING_FG,
                )
                shown_unconfigured = True

            card = CompanyCard(
                parent       = self._list_frame,
                company      = co,
                on_select    = self._on_card_select,
                on_sync      = self._on_single_sync,
                on_schedule  = self._on_single_schedule,
                on_configure = self._on_configure_company,
                selected     = co.name in self.state.selected_companies,
            )
            bg = Color.BG_TABLE_ODD if i % 2 == 0 else Color.BG_TABLE_EVEN
            card.configure(bg=bg)
            card.pack(fill="x")
            self._cards[co.name] = card

            tk.Frame(self._list_frame, bg=Color.BORDER_LIGHT, height=1).pack(fill="x")

    def _get_filtered_companies(self, filter_text: str = "") -> list:
        """Return filtered + visibility-filtered company list."""
        companies = list(self.state.companies.values())

        if filter_text:
            ft = filter_text.lower()
            companies = [c for c in companies if ft in c.name.lower()]

        if not self._show_all_var.get():
            companies = [c for c in companies
                         if c.status != CompanyStatus.NOT_CONFIGURED]

        return companies

    def _render_empty_state(self, filter_text: str = ""):
        if filter_text:
            msg = "No companies match your search."
        elif not self._show_all_var.get():
            msg = "No configured companies.\nClick ⟳ Refresh to load, or show unconfigured companies."
        else:
            msg = "No companies found.\nMake sure Tally is running, then click ⟳ Refresh."

        tk.Label(
            self._list_frame, text=msg,
            font=Font.BODY, bg=Color.BG_CARD, fg=Color.TEXT_MUTED,
            justify="center", pady=60,
        ).pack(fill="both", expand=True)

    def _add_section_header(self, text: str, fg: str):
        f = tk.Frame(self._list_frame, bg=Color.BG_TABLE_HEADER)
        f.pack(fill="x")
        tk.Label(
            f, text=text, font=Font.BODY_SM_BOLD,
            bg=Color.BG_TABLE_HEADER, fg=fg,
            padx=Spacing.LG, pady=5, anchor="w",
        ).pack(fill="x")

    # ─────────────────────────────────────────────────────────────────────────
    #  Configure company
    # ─────────────────────────────────────────────────────────────────────────
    def _on_configure_company(self, name: str):
        co = self.state.get_company(name)
        if not co:
            return

        from gui.components.configure_company_dialog import ConfigureCompanyDialog

        dialog = ConfigureCompanyDialog(
            parent  = self.winfo_toplevel(),
            company = co,
            app     = self.app,
            state   = self.state,
        )
        self.wait_window(dialog)

        if dialog.saved:
            self._render_cards(
                filter_text=self._filter_var.get().strip(),
                force_rebuild=True,
            )
            self._update_summary()

    # ─────────────────────────────────────────────────────────────────────────
    #  Selection
    # ─────────────────────────────────────────────────────────────────────────
    def _on_card_select(self, name: str, selected: bool):
        co = self.state.companies.get(name)
        if co and co.status == CompanyStatus.NOT_CONFIGURED:
            if name in self._cards:
                self._cards[name].set_selected(False)
            return

        sel = self.state.selected_companies
        if selected:
            if name not in sel:
                sel.append(name)
        else:
            self.state.selected_companies = [n for n in sel if n != name]
        self._update_action_bar()

    def _select_all(self):
        self.state.selected_companies = [
            n for n, c in self.state.companies.items()
            if c.status != CompanyStatus.NOT_CONFIGURED
        ]
        for name, card in self._cards.items():
            co = self.state.companies.get(name)
            card.set_selected(bool(co and co.status != CompanyStatus.NOT_CONFIGURED))
        self._update_action_bar()

    def _deselect_all(self):
        self.state.selected_companies = []
        for card in self._cards.values():
            card.set_selected(False)
        self._update_action_bar()

    def _update_action_bar(self):
        configured_selected = [
            n for n in self.state.selected_companies
            if (co := self.state.companies.get(n)) and co.status != CompanyStatus.NOT_CONFIGURED
        ]
        self.state.selected_companies = configured_selected

        n = len(configured_selected)
        if n == 0:
            self._selection_lbl.configure(text="No companies selected")
            self._sync_sel_btn.configure(state="disabled")
            self._sched_sel_btn.configure(state="disabled")
        else:
            plural = "company" if n == 1 else "companies"
            self._selection_lbl.configure(text=f"{n} {plural} selected")
            self._sync_sel_btn.configure(state="normal")
            self._sched_sel_btn.configure(state="normal")

    def _update_summary(self):
        total        = len(self.state.companies)
        configured   = len(self.state.configured_companies())
        unconfigured = total - configured
        tally_open   = sum(
            1 for c in self.state.companies.values()
            if getattr(c, 'tally_open', False)
        )
        syncing = sum(
            1 for c in self.state.companies.values()
            if c.status == CompanyStatus.SYNCING
        )
        parts = [f"{total} total", f"{configured} configured"]
        if unconfigured:
            parts.append(f"{unconfigured} not configured")
        if tally_open:
            parts.append(f"{tally_open} open in Tally")
        if syncing:
            parts.append(f"{syncing} syncing")
        self._summary_lbl.configure(text="  ·  ".join(parts))

    # ─────────────────────────────────────────────────────────────────────────
    #  Button handlers
    # ─────────────────────────────────────────────────────────────────────────
    def _on_refresh(self):
        self._refresh_btn.configure(text="⟳  Refreshing...", state="disabled")

        def worker():
            try:
                self.app._load_companies_from_db(self.state.db_engine)
                try:
                    from gui.controllers.company_controller import CompanyController
                    CompanyController(self.state).load_scheduler_config()
                except Exception:
                    pass
                self.app.post("companies_loaded", None)
            except Exception as e:
                self.app.post("error", f"Refresh failed: {e}")
            finally:
                self.after(0, lambda: self._refresh_btn.configure(
                    text="⟳  Refresh", state="normal"
                ))

        threading.Thread(target=worker, daemon=True).start()

    def _on_sync_selected(self):
        if not self.state.selected_companies:
            messagebox.showwarning("No Selection", "Please select at least one company.")
            return

        # ── Block manual sync if SyncQueueController round is active ─────────
        # Starting SyncController while the queue is running would launch TWO
        # Tally automation sessions simultaneously — PyAutoGUI would fight itself,
        # clicking on both screens at once and corrupting both syncs.
        sync_q = getattr(self.app, '_sync_queue_controller', None)
        if sync_q and not sync_q.is_idle:
            current = sync_q.current_company or "?"
            waiting = list(sync_q.queued_companies)
            detail  = f"Currently syncing: {current}"
            if waiting:
                detail += f"\nWaiting: {', '.join(waiting)}"
            messagebox.showwarning(
                "Scheduled Sync in Progress",
                f"A scheduled sync round is currently running.\n\n"
                f"{detail}\n\n"
                "Please wait for the round to finish, then run a manual sync.\n"
                "You can monitor progress on the Scheduler page.",
            )
            return

        if self.state.sync_active:
            messagebox.showwarning("Sync Running", "A sync is already in progress.")
            return

        needs_snapshot = [
            n for n in self.state.selected_companies
            if (co := self.state.get_company(n)) and not co.is_initial_done
        ]

        if needs_snapshot:
            names = ", ".join(needs_snapshot[:3])
            if len(needs_snapshot) > 3:
                names += f" + {len(needs_snapshot)-3} more"
            n_snap = len(needs_snapshot)
            label  = "company has" if n_snap == 1 else "companies have"
            msg    = (
                f"{n_snap} selected {label} not completed an initial snapshot:\n\n"
                f"{names}\n\n"
                f"Run a full snapshot for all selected companies first?\n"
                f"(Recommended — choose No to run incremental anyway)"
            )
            ans = messagebox.askyesno("Initial Snapshot Required", msg)
            if ans:
                from gui.state import SyncMode
                dates = [
                    co.starting_from for n in self.state.selected_companies
                    if (co := self.state.get_company(n)) and co.starting_from
                ]
                self.state.sync_mode      = SyncMode.SNAPSHOT
                self.state.sync_from_date = min(dates) if dates else None

        self.navigate("sync")

    def _on_single_sync(self, name: str):
        # ── Block manual sync if SyncQueueController round is active ─────────
        sync_q = getattr(self.app, '_sync_queue_controller', None)
        if sync_q and not sync_q.is_idle:
            current = sync_q.current_company or "?"
            messagebox.showwarning(
                "Scheduled Sync in Progress",
                f"A scheduled sync is currently running ({current}).\n\n"
                "Please wait for it to finish before starting a manual sync.\n"
                "Check the Scheduler page for progress.",
            )
            return

        if self.state.sync_active:
            messagebox.showwarning("Sync Running", "A sync is already in progress.")
            return

        co = self.state.get_company(name)
        if not co:
            return
        self.state.selected_companies = [name]
        for n, card in self._cards.items():
            card.set_selected(n == name)
        self._update_action_bar()

        if not co.is_initial_done:
            self._handle_initial_snapshot_flow(name)
        else:
            self.navigate("sync")

    def _handle_initial_snapshot_flow(self, name: str):
        from gui.components.initial_snapshot_dialog import InitialSnapshotDialog

        co     = self.state.get_company(name)
        dialog = InitialSnapshotDialog(self.winfo_toplevel(), co)
        self.wait_window(dialog)

        if dialog.result is None:
            return

        if dialog.result == "snapshot":
            from gui.state import SyncMode
            self.state.sync_mode      = SyncMode.SNAPSHOT
            self.state.sync_from_date = co.starting_from

        self.navigate("sync")

    def _on_schedule_selected(self):
        if not self.state.selected_companies:
            messagebox.showwarning("No Selection", "Please select at least one company.")
            return
        self.navigate("scheduler")

    def _on_single_schedule(self, name: str):
        self.state.selected_companies = [name]
        for n, card in self._cards.items():
            card.set_selected(n == name)
        self._update_action_bar()
        self.navigate("scheduler")

    # ─────────────────────────────────────────────────────────────────────────
    #  Filter
    # ─────────────────────────────────────────────────────────────────────────
    def _on_filter_change(self, *args):
        self._render_cards(filter_text=self._filter_var.get().strip())

    # ─────────────────────────────────────────────────────────────────────────
    #  AppState event callbacks — Phase 2: in-place update, no rebuild
    # ─────────────────────────────────────────────────────────────────────────
    def _on_company_updated(self, name: str, company: CompanyState):
        def _do():
            if name in self._cards:
                card = self._cards[name]
                # Update status badge
                card.update_status(company.status)
                # Update snapshot badge — this is the one that stayed stale
                card.update_snapshot(company.is_initial_done)
                # Update last-sync time in the meta row
                if company.last_sync_time:
                    card.update_sync_time(company.last_sync_time)
            self._update_summary()
        self.after(0, _do)

    def _on_company_progress(self, name: str, pct: float, label: str):
        def _do():
            if name in self._cards:
                # Update progress bar only — no widget rebuild
                self._cards[name].update_progress(pct, label)
        self.after(0, _do)

    def _on_sync_finished(self):
        def _do():
            self._update_summary()
            # Reset progress bars in place
            for card in self._cards.values():
                card.update_progress(0.0, "")
            # Also clear queue state labels (round just ended)
            self._refresh_cards_queue_state()
        self.after(0, _do)

    def _refresh_cards_queue_state(self):
        """
        Update all visible card meta labels to show live queue position.
        Called on every queue_updated event from app.py and on sync_finished.
        """
        sync_q = getattr(self.app, '_sync_queue_controller', None)
        if sync_q is None:
            return
        current = sync_q.current_company
        waiting = list(sync_q.queued_companies)
        for card in self._cards.values():
            card.update_queue_state(current, waiting)

    # ─────────────────────────────────────────────────────────────────────────
    def on_show(self):
        """Called every time this page is navigated to."""
        self._update_summary()
        self._update_action_bar()
        if self.state.companies:
            self._render_cards(filter_text=self._filter_var.get().strip())