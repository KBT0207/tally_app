"""
gui/components/sync_progress_panel.py
=======================================
Shows real-time sync progress for one company:
  - Company name + status badge
  - Progress bar (0–100%)
  - Current step label (e.g. "Syncing sales vouchers...")
  - Mini log (last 5 lines)
  - Cancel button per company
"""

import tkinter as tk
from gui.styles import Color, Font, Spacing
from gui.components.status_badge import StatusBadge


class SyncProgressPanel(tk.Frame):
    """
    One panel per company during an active sync run.
    Updated by sync_controller via queue → home_page event system.
    """

    def __init__(self, parent, company_name: str, on_cancel=None, **kwargs):
        super().__init__(
            parent,
            bg=Color.BG_CARD,
            relief="flat",
            highlightthickness=1,
            highlightbackground=Color.BORDER,
            **kwargs,
        )
        self.company_name = company_name
        self._on_cancel   = on_cancel
        self._log_lines   = []
        self._build()

    def _build(self):
        pad = tk.Frame(self, bg=Color.BG_CARD, padx=Spacing.LG, pady=Spacing.MD)
        pad.pack(fill="both", expand=True)
        pad.columnconfigure(0, weight=1)

        # ── Header: name + badge + cancel ────────────────
        hdr = tk.Frame(pad, bg=Color.BG_CARD)
        hdr.grid(row=0, column=0, sticky="ew", pady=(0, Spacing.SM))
        hdr.columnconfigure(0, weight=1)

        tk.Label(
            hdr, text=self.company_name,
            font=Font.LABEL_BOLD, bg=Color.BG_CARD, fg=Color.TEXT_PRIMARY, anchor="w",
        ).grid(row=0, column=0, sticky="w")

        self._badge = StatusBadge(hdr, status="Syncing")
        self._badge.grid(row=0, column=1, padx=(Spacing.MD, Spacing.MD))

        if self._on_cancel:
            tk.Button(
                hdr, text="✖",
                font=Font.BUTTON_SM,
                bg=Color.DANGER_BG, fg=Color.DANGER_FG,
                relief="flat", bd=0, padx=6, pady=2,
                cursor="hand2",
                command=lambda: self._on_cancel(self.company_name),
            ).grid(row=0, column=2)

        # ── Step label ────────────────────────────────────
        self._step_lbl = tk.Label(
            pad, text="Waiting to start...",
            font=Font.BODY_SM, bg=Color.BG_CARD,
            fg=Color.TEXT_SECONDARY, anchor="w",
        )
        self._step_lbl.grid(row=1, column=0, sticky="ew")

        # ── Progress bar ──────────────────────────────────
        prog_frame = tk.Frame(pad, bg=Color.BG_CARD)
        prog_frame.grid(row=2, column=0, sticky="ew", pady=(Spacing.XS, Spacing.SM))

        self._prog_canvas = tk.Canvas(
            prog_frame, height=10,
            bg=Color.PROGRESS_BG, highlightthickness=0, bd=0,
        )
        self._prog_canvas.pack(fill="x", side="left", expand=True)
        self._prog_bar = self._prog_canvas.create_rectangle(
            0, 0, 0, 10, fill=Color.PROGRESS_FILL, width=0,
        )

        self._pct_lbl = tk.Label(
            prog_frame, text="0%",
            font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY, width=5,
        )
        self._pct_lbl.pack(side="left", padx=(Spacing.SM, 0))

        # ── Mini log ──────────────────────────────────────
        log_frame = tk.Frame(
            pad, bg=Color.LOG_BG,
            relief="flat",
            highlightthickness=1, highlightbackground=Color.BORDER_LIGHT,
        )
        log_frame.grid(row=3, column=0, sticky="ew")

        self._log_text = tk.Text(
            log_frame,
            height=4,
            font=Font.MONO_SM,
            bg=Color.LOG_BG,
            fg=Color.LOG_INFO,
            relief="flat", bd=0,
            state="disabled",
            wrap="word",
        )
        self._log_text.pack(fill="both", expand=True, padx=4, pady=4)

        # Log text tags
        self._log_text.tag_config("INFO",    foreground=Color.LOG_INFO)
        self._log_text.tag_config("SUCCESS", foreground=Color.LOG_SUCCESS)
        self._log_text.tag_config("WARNING", foreground=Color.LOG_WARNING)
        self._log_text.tag_config("ERROR",   foreground=Color.LOG_ERROR)
        self._log_text.tag_config("DEBUG",   foreground=Color.LOG_DEBUG)

    # ── Public API ────────────────────────────────────────────────────────────
    def set_progress(self, pct: float, label: str = ""):
        """Update progress bar and step label."""
        pct = max(0.0, min(100.0, pct))
        self._step_lbl.configure(text=label or f"{pct:.0f}%")
        self._pct_lbl.configure(text=f"{pct:.0f}%")

        # Update bar width on next layout cycle
        self._prog_canvas.update_idletasks()
        w = self._prog_canvas.winfo_width()
        bar_w = int(w * pct / 100)
        fill  = Color.PROGRESS_SUCCESS if pct >= 100 else Color.PROGRESS_FILL
        self._prog_canvas.coords(self._prog_bar, 0, 0, bar_w, 10)
        self._prog_canvas.itemconfig(self._prog_bar, fill=fill)

    def set_status(self, status: str):
        self._badge.set_status(status)

    def append_log(self, line: str, level: str = "INFO"):
        """Append a log line. level: INFO | SUCCESS | WARNING | ERROR | DEBUG"""
        self._log_text.configure(state="normal")
        self._log_text.insert("end", line + "\n", level.upper())
        self._log_text.see("end")
        # Keep only last 200 lines
        lines = int(self._log_text.index("end-1c").split(".")[0])
        if lines > 200:
            self._log_text.delete("1.0", f"{lines - 200}.0")
        self._log_text.configure(state="disabled")

    def mark_done(self, success: bool = True):
        status = "Sync Done" if success else "Sync Error"
        self._badge.set_status(status)
        if success:
            self.set_progress(100.0, "✓ Complete")
        else:
            self._step_lbl.configure(text="✗ Failed — see log", fg=Color.DANGER)

    def mark_waiting(self):
        self._badge.set_status("Configured")
        self._step_lbl.configure(text="⏳ Waiting...")
        self._pct_lbl.configure(text="0%")
        self._prog_canvas.coords(self._prog_bar, 0, 0, 0, 10)