"""
gui/components/company_card.py
================================
A single company row card for the home page list.

Configured companies:
┌────────────────────────────────────────────────────────────────────────┐
│ [☑]  ABC Traders Pvt Ltd   ● Tally   ● Configured    [▶ Sync] [⏰]   │
│      Last sync: 20-Feb-2026 10:30  ·  alter_id: 4521  ·  Books: Apr  │
└────────────────────────────────────────────────────────────────────────┘

Not-configured (Tally-only) companies:
┌────────────────────────────────────────────────────────────────────────┐
│ [☑]  Beta Corp Pvt Ltd     ● Tally   ○ Not Configured [⚙ Configure]  │
│      Books from: 01 Apr 2024  ·  Click Configure to set up            │
└────────────────────────────────────────────────────────────────────────┘
"""

import tkinter as tk
from datetime import datetime
from typing import Callable

from gui.state  import CompanyState, CompanyStatus
from gui.styles import Color, Font, Spacing, STATUS_STYLE
from gui.components.status_badge import StatusBadge


class CompanyCard(tk.Frame):

    def __init__(
        self,
        parent,
        company:      CompanyState,
        on_select:    Callable,
        on_sync:      Callable,
        on_schedule:  Callable,
        on_configure: Callable = None,   # NEW — for NOT_CONFIGURED companies
        selected:     bool = False,
        **kwargs,
    ):
        super().__init__(parent, bg=Color.BG_CARD, relief="flat", bd=0, **kwargs)
        self.company      = company
        self.on_select    = on_select
        self.on_sync      = on_sync
        self.on_schedule  = on_schedule
        self.on_configure = on_configure
        self._selected_var = tk.BooleanVar(value=selected)
        self._bg_frames    = []
        self._prog_lbl     = None
        self._prog_canvas  = None
        self._prog_bar     = None
        self._meta_lbl     = None
        self._snap_badge   = None   # reference so update_snapshot() can swap it
        self._build()
        self._bind_hover()

    # ─────────────────────────────────────────────────────────────────────────
    def _build(self):
        co            = self.company
        is_configured = (co.status != CompanyStatus.NOT_CONFIGURED)
        tally_open    = getattr(co, 'tally_open', False)

        outer = tk.Frame(self, bg=Color.BG_CARD, padx=Spacing.LG, pady=10)
        outer.pack(fill="x")
        outer.columnconfigure(1, weight=1)
        self._bg_frames = [self, outer]

        # ── Checkbox ──────────────────────────────────────
        # NOT_CONFIGURED companies cannot be selected for Sync/Schedule —
        # show a disabled, always-unchecked checkbox to make this clear.
        chk = tk.Checkbutton(
            outer,
            variable=self._selected_var,
            bg=Color.BG_CARD,
            activebackground=Color.BG_CARD,
            disabledforeground=Color.TEXT_MUTED,
            relief="flat", bd=0,
            command=self._on_toggle,
            state="disabled" if not is_configured else "normal",
        )
        chk.grid(row=0, column=0, rowspan=2, sticky="ns", padx=(0, Spacing.MD))
        self._chk = chk   # keep ref so set_selected() can skip it when disabled
        self._bg_frames.append(chk)

        # ── Company name row ──────────────────────────────
        name_row = tk.Frame(outer, bg=Color.BG_CARD)
        name_row.grid(row=0, column=1, sticky="ew")
        self._bg_frames.append(name_row)

        self._name_lbl = tk.Label(
            name_row, text=co.name,
            font=Font.LABEL_BOLD, bg=Color.BG_CARD,
            fg=Color.TEXT_MUTED if not is_configured else Color.TEXT_PRIMARY,
            anchor="w",
        )
        self._name_lbl.pack(side="left")
        self._bg_frames.append(self._name_lbl)

        # Tally open/offline pill
        if tally_open:
            tp = tk.Label(
                name_row, text="● Tally",
                font=Font.BADGE,
                bg=Color.SUCCESS_BG, fg=Color.SUCCESS_FG,
                padx=5, pady=1,
            )
            tp.pack(side="left", padx=(Spacing.SM, 0))
            self._bg_frames.append(tp)
        elif is_configured:
            tp = tk.Label(
                name_row, text="○ Tally offline",
                font=Font.BADGE,
                bg=Color.MUTED_BG, fg=Color.MUTED_FG,
                padx=5, pady=1,
            )
            tp.pack(side="left", padx=(Spacing.SM, 0))
            self._bg_frames.append(tp)

        # Scheduled pill
        if co.schedule_enabled:
            sp = tk.Label(
                name_row, text="⏰ Scheduled",
                font=Font.BADGE, bg=Color.INFO_BG, fg=Color.INFO_FG,
                padx=5, pady=1,
            )
            sp.pack(side="left", padx=(Spacing.SM, 0))
            self._bg_frames.append(sp)

        # Initial snapshot badge (configured companies only)
        if is_configured:
            if co.is_initial_done:
                snap_badge = tk.Label(
                    name_row, text="✓ Snapshot",
                    font=Font.BADGE,
                    bg=Color.SUCCESS_BG, fg=Color.SUCCESS_FG,
                    padx=5, pady=1,
                )
            else:
                snap_badge = tk.Label(
                    name_row, text="⚠ No Snapshot",
                    font=Font.BADGE,
                    bg=Color.WARNING_BG, fg=Color.WARNING_FG,
                    padx=5, pady=1,
                )
            snap_badge.pack(side="left", padx=(Spacing.SM, 0))
            self._snap_badge = snap_badge          # ← store reference
            self._bg_frames.append(snap_badge)

        # ── Meta row ─────────────────────────────────────
        meta = tk.Frame(outer, bg=Color.BG_CARD)
        meta.grid(row=1, column=1, sticky="ew", pady=(2, 0))
        self._bg_frames.append(meta)

        if is_configured:
            self._meta_lbl = tk.Label(
                meta, text=f"Last sync: {self._fmt_sync_time(co.last_sync_time)}",
                font=Font.BODY_SM, bg=Color.BG_CARD,
                fg=Color.TEXT_SECONDARY, anchor="w",
            )
            self._meta_lbl.pack(side="left")
            self._bg_frames.append(self._meta_lbl)

            if co.last_alter_id:
                for w in (
                    tk.Label(meta, text="  ·  ", font=Font.BODY_SM,
                             bg=Color.BG_CARD, fg=Color.TEXT_MUTED),
                    tk.Label(meta, text=f"alter_id: {co.last_alter_id:,}",
                             font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.TEXT_MUTED),
                ):
                    w.pack(side="left")
                    self._bg_frames.append(w)

            if co.starting_from:
                for w in (
                    tk.Label(meta, text="  ·  ", font=Font.BODY_SM,
                             bg=Color.BG_CARD, fg=Color.TEXT_MUTED),
                    tk.Label(meta, text=f"Books from: {self._fmt_date_str(co.starting_from)}",
                             font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.TEXT_MUTED),
                ):
                    w.pack(side="left")
                    self._bg_frames.append(w)
        else:
            # Not configured — show hint from Tally data
            parts = []
            ref = co.books_from or co.starting_from
            if ref:
                parts.append(f"Books from: {self._fmt_date_str(ref)}")
            parts.append("Click Configure to add this company")

            self._meta_lbl = tk.Label(
                meta, text="  ·  ".join(parts),
                font=Font.BODY_SM, bg=Color.BG_CARD,
                fg=Color.TEXT_MUTED, anchor="w",
            )
            self._meta_lbl.pack(side="left")
            self._bg_frames.append(self._meta_lbl)

        # ── Status badge ──────────────────────────────────
        badge_wrap = tk.Frame(outer, bg=Color.BG_CARD)
        badge_wrap.grid(row=0, column=2, rowspan=2,
                        padx=(Spacing.LG, Spacing.MD), sticky="ns")
        self._bg_frames.append(badge_wrap)
        self._badge = StatusBadge(badge_wrap, status=co.status)
        self._badge.pack(anchor="center", expand=True)

        # ── Progress column (configured only) ─────────────
        prog_wrap = tk.Frame(outer, bg=Color.BG_CARD, width=140)
        prog_wrap.grid(row=0, column=3, rowspan=2,
                       padx=(0, Spacing.MD), sticky="ns")
        prog_wrap.grid_propagate(False)
        self._bg_frames.append(prog_wrap)

        if is_configured:
            self._prog_lbl = tk.Label(
                prog_wrap, text="", font=Font.BODY_SM,
                bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY,
            )
            self._prog_lbl.pack(anchor="w")
            self._bg_frames.append(self._prog_lbl)

            self._prog_canvas = tk.Canvas(
                prog_wrap, height=6, width=130,
                bg=Color.PROGRESS_BG, highlightthickness=0, bd=0,
            )
            self._prog_canvas.pack(anchor="w", pady=(2, 0))
            self._prog_bar = self._prog_canvas.create_rectangle(
                0, 0, 0, 6, fill=Color.PROGRESS_FILL, width=0,
            )
            self._bg_frames.append(self._prog_canvas)

        # ── Action buttons ────────────────────────────────
        btn_wrap = tk.Frame(outer, bg=Color.BG_CARD)
        btn_wrap.grid(row=0, column=4, rowspan=2, sticky="ns")
        self._bg_frames.append(btn_wrap)

        if is_configured:
            self._sync_btn = tk.Button(
                btn_wrap,
                text="▶ Sync",
                font=Font.BUTTON_SM,
                bg=Color.PRIMARY, fg=Color.TEXT_WHITE,
                relief="flat", bd=0,
                padx=Spacing.MD, pady=Spacing.XS,
                cursor="hand2",
                command=self._on_sync_click,
            )
            self._sync_btn.pack(pady=(0, Spacing.XS))

            # Bottom row: Schedule + Edit side by side
            sub_row = tk.Frame(btn_wrap, bg=Color.BG_CARD)
            sub_row.pack()
            self._bg_frames.append(sub_row)

            tk.Button(
                sub_row,
                text="⏰",
                font=Font.BUTTON_SM,
                bg=Color.INFO_BG, fg=Color.INFO_FG,
                relief="flat", bd=0,
                padx=Spacing.SM, pady=Spacing.XS,
                cursor="hand2",
                command=self._on_sched_click,
            ).pack(side="left", padx=(0, 4))

            tk.Button(
                sub_row,
                text="✎",
                font=Font.BUTTON_SM,
                bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY,
                relief="solid", bd=1,
                padx=Spacing.SM, pady=Spacing.XS,
                cursor="hand2",
                command=self._on_configure_click,
            ).pack(side="left")
        else:
            # Configure button — highlighted more when Tally is open
            cfg_bg = Color.PRIMARY      if tally_open else Color.WARNING_BG
            cfg_fg = Color.TEXT_WHITE   if tally_open else Color.WARNING_FG

            tk.Button(
                btn_wrap,
                text="⚙  Configure",
                font=Font.BUTTON_SM,
                bg=cfg_bg, fg=cfg_fg,
                relief="flat", bd=0,
                padx=Spacing.MD, pady=Spacing.SM,
                cursor="hand2",
                command=self._on_configure_click,
            ).pack()

    # ─────────────────────────────────────────────────────────────────────────
    def _bind_hover(self):
        def on_enter(e):
            for w in self._bg_frames:
                try: w.configure(bg=Color.BG_CARD_HOVER)
                except Exception: pass
            if self._prog_canvas:
                self._prog_canvas.configure(bg=Color.BG_CARD_HOVER)

        def on_leave(e):
            for w in self._bg_frames:
                try: w.configure(bg=Color.BG_CARD)
                except Exception: pass
            if self._prog_canvas:
                self._prog_canvas.configure(bg=Color.BG_CARD)

        for w in [self] + self._bg_frames:
            try:
                w.bind("<Enter>", on_enter)
                w.bind("<Leave>", on_leave)
            except Exception:
                pass

    # ─────────────────────────────────────────────────────────────────────────
    def _on_toggle(self):
        # Extra safety — should never fire for disabled checkboxes, but guard anyway
        if self.company.status == "Not Configured":
            self._selected_var.set(False)
            return
        self.on_select(self.company.name, self._selected_var.get())

    def _on_sync_click(self):
        self.on_sync(self.company.name)

    def _on_sched_click(self):
        self.on_schedule(self.company.name)

    def _on_configure_click(self):
        if self.on_configure:
            self.on_configure(self.company.name)

    # ─────────────────────────────────────────────────────────────────────────
    #  Public update API
    # ─────────────────────────────────────────────────────────────────────────
    def update_status(self, status: str):
        self._badge.set_status(status)

    def update_snapshot(self, is_done: bool):
        """Flip the snapshot badge between ✓ Snapshot and ⚠ No Snapshot in place."""
        if not self._snap_badge:
            return
        if is_done:
            self._snap_badge.configure(
                text="✓ Snapshot",
                bg=Color.SUCCESS_BG, fg=Color.SUCCESS_FG,
            )
        else:
            self._snap_badge.configure(
                text="⚠ No Snapshot",
                bg=Color.WARNING_BG, fg=Color.WARNING_FG,
            )

    def update_progress(self, pct: float, label: str = ""):
        if not self._prog_canvas:
            return
        self._prog_lbl.configure(text=label or (f"{pct:.0f}%" if pct > 0 else ""))
        bar_w = int(130 * min(pct, 100) / 100)
        fill  = Color.PROGRESS_SUCCESS if pct >= 100 else Color.PROGRESS_FILL
        self._prog_canvas.coords(self._prog_bar, 0, 0, bar_w, 6)
        self._prog_canvas.itemconfig(self._prog_bar, fill=fill)

    def update_sync_time(self, dt):
        if self._meta_lbl:
            self._meta_lbl.configure(text=f"Last sync: {self._fmt_sync_time(dt)}")

    def update_queue_state(self, current_company: str, queued: list):
        """
        Update the card's meta label to show live queue position.
        Called from home_page on every 'queue_updated' event.

        Shows:
          • "⟳ Syncing now..."   — if this company is currently running
          • "⏳ Queue position N" — if this company is waiting
          • Restores last_sync_time display when queue is idle
        """
        if not self._meta_lbl:
            return
        name = self.company.name
        if name == current_company:
            self._meta_lbl.configure(
                text="⟳ Syncing now...",
                fg=Color.PRIMARY,
            )
        elif name in queued:
            pos = queued.index(name) + 1
            self._meta_lbl.configure(
                text=f"⏳ Queue position {pos}",
                fg=Color.INFO_FG,
            )
        else:
            # Not in queue — restore normal last_sync_time display
            self._meta_lbl.configure(
                text=f"Last sync: {self._fmt_sync_time(self.company.last_sync_time)}",
                fg=Color.TEXT_SECONDARY,
            )

    def set_selected(self, value: bool):
        # NOT_CONFIGURED companies are never selectable — skip silently
        if self.company.status == "Not Configured":
            self._selected_var.set(False)
            return
        self._selected_var.set(value)

    def is_selected(self) -> bool:
        return self._selected_var.get()

    # ─────────────────────────────────────────────────────────────────────────
    @staticmethod
    def _fmt_sync_time(dt) -> str:
        if not dt:
            return "Never"
        try:
            if isinstance(dt, str):
                dt = datetime.fromisoformat(dt)
            return dt.strftime("%d %b %Y  %H:%M")
        except Exception:
            return str(dt)

    @staticmethod
    def _fmt_date_str(s: str) -> str:
        try:
            return datetime.strptime(str(s)[:8], "%Y%m%d").strftime("%d %b %Y")
        except Exception:
            return str(s)