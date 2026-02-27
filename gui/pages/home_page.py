"""
gui/pages/home_page.py
=======================
Home page — full company list with:
  - ALL companies from Tally shown on first open (even not-yet-configured)
  - Configured companies (from DB) with status, last sync, alter_id
  - Not-configured companies (Tally-only) with a Configure button
  - Tally open/offline indicator per card
  - Checkboxes for single / multi-select
  - Per-card Sync button (quick single company sync)
  - Bulk action bar: Sync Selected, Schedule Selected
  - Select All / Deselect All
  - Search/filter bar
  - Refresh button (re-queries DB + Tally)
  - Live progress bar per card during sync
  - Status auto-refreshes via AppState event system
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

        # Filter toggle: show all / configured only
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
        self._canvas.bind_all(
            "<MouseWheel>",
            lambda e: self._canvas.yview_scroll(int(-1 * (e.delta / 120)), "units")
        )

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
    #  Render cards
    # ─────────────────────────────────────────────────────────────────────────
    def refresh_companies(self):
        """Called by app.py after DB + Tally load completes."""
        self._render_cards()
        self._update_summary()

    def _render_cards(self, filter_text: str = ""):
        for w in self._list_frame.winfo_children():
            w.destroy()
        self._cards.clear()

        companies = list(self.state.companies.values())

        # Filter by search text
        if filter_text:
            ft = filter_text.lower()
            companies = [c for c in companies if ft in c.name.lower()]

        # Optionally hide unconfigured
        if not self._show_all_var.get():
            companies = [c for c in companies
                         if c.status != CompanyStatus.NOT_CONFIGURED]

        if not companies:
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
            return

        # Sort: configured first (alphabetical within each group)
        companies.sort(key=lambda c: (
            0 if c.status != CompanyStatus.NOT_CONFIGURED else 1,
            c.name.lower(),
        ))

        # Section headers
        shown_configured    = False
        shown_unconfigured  = False

        for i, co in enumerate(companies):
            is_configured = (co.status != CompanyStatus.NOT_CONFIGURED)

            # Section divider labels
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

    def _add_section_header(self, text: str, fg: str):
        """Insert a subtle section label between groups."""
        f = tk.Frame(self._list_frame, bg=Color.BG_TABLE_HEADER)
        f.pack(fill="x")
        tk.Label(
            f, text=text,
            font=Font.BODY_SM_BOLD,
            bg=Color.BG_TABLE_HEADER, fg=fg,
            padx=Spacing.LG, pady=5, anchor="w",
        ).pack(fill="x")

    # ─────────────────────────────────────────────────────────────────────────
    #  Configure company (NOT_CONFIGURED → dialog → save DB → refresh card)
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
            # Re-render the list so the card changes to Configured view
            self._render_cards(filter_text=self._filter_var.get().strip())
            self._update_summary()

    # ─────────────────────────────────────────────────────────────────────────
    #  Selection
    # ─────────────────────────────────────────────────────────────────────────
    def _on_card_select(self, name: str, selected: bool):
        # Silently reject selection of not-configured companies
        co = self.state.companies.get(name)
        if co and co.status == CompanyStatus.NOT_CONFIGURED:
            # Force the card's checkbox back to unchecked
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
        # Only configured companies can be selected for sync/schedule
        self.state.selected_companies = [
            n for n, c in self.state.companies.items()
            if c.status != CompanyStatus.NOT_CONFIGURED
        ]
        for name, card in self._cards.items():
            co = self.state.companies.get(name)
            # Only tick cards for configured companies; leave NOT_CONFIGURED unchecked
            card.set_selected(bool(co and co.status != CompanyStatus.NOT_CONFIGURED))
        self._update_action_bar()

    def _deselect_all(self):
        self.state.selected_companies = []
        for card in self._cards.values():
            card.set_selected(False)
        self._update_action_bar()

    def _update_action_bar(self):
        # Only configured companies can actually be synced or scheduled
        configured_selected = [
            n for n in self.state.selected_companies
            if (co := self.state.companies.get(n)) and co.status != CompanyStatus.NOT_CONFIGURED
        ]
        # Keep selected_companies clean — strip any NOT_CONFIGURED that slipped in
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
        parts = [
            f"{total} total",
            f"{configured} configured",
        ]
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
                # Re-apply scheduler config to the brand-new CompanyState objects,
                # then broadcast so ALL pages (home + scheduler) refresh together.
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
        if self.state.sync_active:
            messagebox.showwarning("Sync Running", "A sync is already in progress.")
            return

        # Check if any selected company needs initial snapshot
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
                # Use the earliest starting_from date among selected
                dates = [
                    co.starting_from for n in self.state.selected_companies
                    if (co := self.state.get_company(n)) and co.starting_from
                ]
                self.state.sync_mode      = SyncMode.SNAPSHOT
                self.state.sync_from_date = min(dates) if dates else None

        self.navigate("sync")

    def _on_single_sync(self, name: str):
        """Called when ▶ Sync is clicked on a single company card."""
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

        # ── Smart sync flow: check initial snapshot ───────
        if not co.is_initial_done:
            self._handle_initial_snapshot_flow(name)
        else:
            self.navigate("sync")

    def _handle_initial_snapshot_flow(self, name: str):
        """
        Show InitialSnapshotDialog if is_initial_done is False.
        User can choose full snapshot or skip to incremental.
        """
        from gui.components.initial_snapshot_dialog import InitialSnapshotDialog

        co     = self.state.get_company(name)
        dialog = InitialSnapshotDialog(self.winfo_toplevel(), co)
        self.wait_window(dialog)

        if dialog.result is None:
            return  # cancelled

        if dialog.result == "snapshot":
            # Force snapshot mode on the state before navigating
            from gui.state import SyncMode
            self.state.sync_mode      = SyncMode.SNAPSHOT
            self.state.sync_from_date = co.starting_from  # use configured start date
        # else incremental — let sync_page decide

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
    #  AppState event callbacks  (always .after(0) for thread safety)
    # ─────────────────────────────────────────────────────────────────────────
    def _on_company_updated(self, name: str, company: CompanyState):
        def _do():
            if name in self._cards:
                self._cards[name].update_status(company.status)
            self._update_summary()
        self.after(0, _do)

    def _on_company_progress(self, name: str, pct: float, label: str):
        def _do():
            if name in self._cards:
                self._cards[name].update_progress(pct, label)
        self.after(0, _do)

    def _on_sync_finished(self):
        def _do():
            self._update_summary()
            for card in self._cards.values():
                card.update_progress(0.0, "")
        self.after(0, _do)

    # ─────────────────────────────────────────────────────────────────────────
    def on_show(self):
        """Called every time this page is navigated to."""
        self._update_summary()
        self._update_action_bar()
        if self.state.companies:
            self._render_cards(filter_text=self._filter_var.get().strip())