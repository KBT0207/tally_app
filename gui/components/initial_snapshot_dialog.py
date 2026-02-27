"""
gui/components/initial_snapshot_dialog.py
==========================================
Smart sync flow dialog shown when user clicks Sync on a company
that has NOT completed its initial snapshot.

Flow:
  1. User clicks ▶ Sync on a company with is_initial_done = False
  2. This dialog appears explaining what initial snapshot is
  3. User chooses:
     a) "Run Initial Snapshot Now"  → sync_page opens in SNAPSHOT mode
     b) "Skip for now"              → sync_page opens normally (incremental)

After sync completes (post-sync hook):
  If is_initial_done just became True → PostSnapshotDialog appears
  Asking: "Set up auto-schedule?" → yes → scheduler page

Also shown as a standalone "post snapshot done" celebration + schedule prompt.
"""

import tkinter as tk
from tkinter import messagebox
from typing import Callable, Optional

from gui.styles import Color, Font, Spacing
from gui.state  import CompanyState, SyncMode


class InitialSnapshotDialog(tk.Toplevel):
    """
    Shown when Sync is clicked on a company where is_initial_done = False.

    result:
      "snapshot"    — run full snapshot first
      "incremental" — skip, go straight to incremental
      None          — cancelled
    """

    def __init__(self, parent, company: CompanyState):
        super().__init__(parent)
        self.title("Initial Snapshot Required")
        self.resizable(False, False)
        self.grab_set()
        self.result: Optional[str] = None
        self._company = company
        self._build()
        self._center(parent)
        self.bind("<Escape>", lambda e: self.destroy())

    def _center(self, parent):
        self.update_idletasks()
        w, h = 480, 340
        px = parent.winfo_rootx() + parent.winfo_width()  // 2
        py = parent.winfo_rooty() + parent.winfo_height() // 2
        self.geometry(f"{w}x{h}+{px - w//2}+{py - h//2}")

    def _build(self):
        co  = self._company
        pad = tk.Frame(self, bg=Color.BG_CARD, padx=28, pady=24)
        pad.pack(fill="both", expand=True)

        # ── Icon + title ──────────────────────────────────
        title_row = tk.Frame(pad, bg=Color.BG_CARD)
        title_row.pack(fill="x", pady=(0, 4))

        tk.Label(
            title_row, text="📥",
            font=(Font.FAMILY, 22), bg=Color.BG_CARD,
        ).pack(side="left", padx=(0, 10))

        tk.Label(
            title_row, text="Initial Snapshot Not Done",
            font=Font.HEADING_4, bg=Color.BG_CARD, fg=Color.TEXT_PRIMARY,
        ).pack(side="left", anchor="w")

        # ── Company name ──────────────────────────────────
        tk.Label(
            pad, text=co.name,
            font=Font.LABEL_BOLD, bg=Color.BG_CARD, fg=Color.PRIMARY,
            anchor="w",
        ).pack(fill="x", pady=(0, 12))

        # ── Divider ───────────────────────────────────────
        tk.Frame(pad, bg=Color.BORDER, height=1).pack(fill="x", pady=(0, 14))

        # ── Explanation ───────────────────────────────────
        tk.Label(
            pad,
            text=(
                "This company has never had a full data pull from Tally.\n\n"
                "An  Initial Snapshot  fetches all historical vouchers from\n"
                f"{'  ' + self._fmt(co.starting_from) if co.starting_from else 'the configured start date'}"
                "  up to today — this may take several minutes.\n\n"
                "After the snapshot is done, all future syncs will be fast\n"
                "incremental updates (only new/changed records)."
            ),
            font=Font.BODY, bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY,
            justify="left", anchor="w", wraplength=420,
        ).pack(fill="x")

        # ── Buttons ───────────────────────────────────────
        btn_row = tk.Frame(pad, bg=Color.BG_CARD)
        btn_row.pack(fill="x", pady=(20, 0))

        tk.Button(
            btn_row, text="Skip — run incremental anyway",
            font=Font.BUTTON_SM,
            bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY,
            relief="solid", bd=1, padx=12, pady=6, cursor="hand2",
            command=self._skip,
        ).pack(side="left")

        tk.Button(
            btn_row, text="📥  Run Initial Snapshot Now",
            font=Font.BUTTON_SM,
            bg=Color.PRIMARY, fg=Color.TEXT_WHITE,
            relief="flat", bd=0, padx=16, pady=6, cursor="hand2",
            command=self._run_snapshot,
        ).pack(side="right")

    def _run_snapshot(self):
        self.result = "snapshot"
        self.destroy()

    def _skip(self):
        self.result = "incremental"
        self.destroy()

    @staticmethod
    def _fmt(s: str) -> str:
        try:
            from datetime import datetime
            return datetime.strptime(str(s)[:8], "%Y%m%d").strftime("%d %b %Y")
        except Exception:
            return str(s) if s else ""


# ─────────────────────────────────────────────────────────────────────────────
class PostSnapshotDialog(tk.Toplevel):
    """
    Shown automatically after a company's initial snapshot completes.
    Celebrates the milestone and offers to set up a schedule.

    result:
      "schedule"   — open scheduler page for this company
      "done"       — just close, do nothing
    """

    def __init__(self, parent, company: CompanyState):
        super().__init__(parent)
        self.title("Initial Snapshot Complete")
        self.resizable(False, False)
        self.grab_set()
        self.result: Optional[str] = None
        self._company = company
        self._build()
        self._center(parent)
        self.bind("<Escape>", lambda e: self._done())

    def _center(self, parent):
        self.update_idletasks()
        w, h = 460, 320
        px = parent.winfo_rootx() + parent.winfo_width()  // 2
        py = parent.winfo_rooty() + parent.winfo_height() // 2
        self.geometry(f"{w}x{h}+{px - w//2}+{py - h//2}")

    def _build(self):
        co  = self._company
        pad = tk.Frame(self, bg=Color.BG_CARD, padx=28, pady=24)
        pad.pack(fill="both", expand=True)

        # ── Success icon + title ──────────────────────────
        title_row = tk.Frame(pad, bg=Color.BG_CARD)
        title_row.pack(fill="x", pady=(0, 4))

        tk.Label(
            title_row, text="✅",
            font=(Font.FAMILY, 22), bg=Color.BG_CARD,
        ).pack(side="left", padx=(0, 10))

        tk.Label(
            title_row, text="Initial Snapshot Complete!",
            font=Font.HEADING_4, bg=Color.BG_CARD, fg=Color.SUCCESS_FG,
        ).pack(side="left", anchor="w")

        # ── Company name ──────────────────────────────────
        tk.Label(
            pad, text=co.name,
            font=Font.LABEL_BOLD, bg=Color.BG_CARD, fg=Color.PRIMARY,
            anchor="w",
        ).pack(fill="x", pady=(0, 12))

        tk.Frame(pad, bg=Color.BORDER, height=1).pack(fill="x", pady=(0, 14))

        # ── Message ───────────────────────────────────────
        tk.Label(
            pad,
            text=(
                "All historical data has been pulled from Tally.\n\n"
                "Future syncs for this company will now use fast\n"
                "incremental updates — only new and changed records.\n\n"
                "Would you like to set up an automatic sync schedule\n"
                "so this company stays up to date automatically?"
            ),
            font=Font.BODY, bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY,
            justify="left", anchor="w", wraplength=400,
        ).pack(fill="x")

        # ── Buttons ───────────────────────────────────────
        btn_row = tk.Frame(pad, bg=Color.BG_CARD)
        btn_row.pack(fill="x", pady=(20, 0))

        tk.Button(
            btn_row, text="Not now",
            font=Font.BUTTON_SM,
            bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY,
            relief="solid", bd=1, padx=12, pady=6, cursor="hand2",
            command=self._done,
        ).pack(side="left")

        tk.Button(
            btn_row, text="⏰  Set Up Auto-Schedule",
            font=Font.BUTTON_SM,
            bg=Color.PRIMARY, fg=Color.TEXT_WHITE,
            relief="flat", bd=0, padx=16, pady=6, cursor="hand2",
            command=self._schedule,
        ).pack(side="right")

    def _schedule(self):
        self.result = "schedule"
        self.destroy()

    def _done(self):
        self.result = "done"
        self.destroy()