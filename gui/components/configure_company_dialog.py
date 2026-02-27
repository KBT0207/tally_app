"""
gui/components/configure_company_dialog.py
============================================
Modal dialog shown when a user clicks "Configure" on a Tally company
that doesn't yet exist in the database.

Collects:
  - Sync start date  (YYYYMMDD — "Books from" in Tally)
  - Optional Tally host/port override (per company)

On Save: calls app.save_company_to_db(), updates CompanyState status.
"""

import tkinter as tk
from tkinter import messagebox
from datetime import datetime, date

from gui.styles import Color, Font, Spacing
from gui.state  import CompanyState, CompanyStatus


def _parse_date(s: str):
    """Try multiple formats → date object, or None."""
    for fmt in ("%d-%b-%Y", "%d/%m/%Y", "%Y-%m-%d", "%Y%m%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(s.strip(), fmt).date()
        except ValueError:
            continue
    return None


def _fmt(d: date) -> str:
    return d.strftime("%d-%b-%Y") if d else ""


def _yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d") if d else ""


class ConfigureCompanyDialog(tk.Toplevel):
    """
    Modal dialog to configure a Tally company for syncing.
    Sets self.saved = True on successful save.
    """

    def __init__(self, parent, company: CompanyState, app, state):
        super().__init__(parent)
        self.title(f"Configure  —  {company.name}")
        self.resizable(False, False)
        self.grab_set()           # modal
        self.saved    = False
        self._company = company
        self._app     = app
        self._state   = state

        self._build()

        # Center over parent
        self.update_idletasks()
        pw = parent.winfo_rootx() + parent.winfo_width()  // 2
        ph = parent.winfo_rooty() + parent.winfo_height() // 2
        w, h = 460, 400
        self.geometry(f"{w}x{h}+{pw - w//2}+{ph - h//2}")

        self.bind("<Return>", lambda e: self._on_save())
        self.bind("<Escape>", lambda e: self.destroy())

    # ─────────────────────────────────────────────────────────────────────────
    def _build(self):
        co  = self._company
        pad = tk.Frame(self, bg=Color.BG_CARD, padx=28, pady=24)
        pad.pack(fill="both", expand=True)

        # ── Title ─────────────────────────────────────────
        tk.Label(
            pad, text="Configure Company",
            font=Font.HEADING_4, bg=Color.BG_CARD, fg=Color.TEXT_PRIMARY,
        ).grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 4))

        # Company name + Tally badge
        name_row = tk.Frame(pad, bg=Color.BG_CARD)
        name_row.grid(row=1, column=0, columnspan=2, sticky="w", pady=(0, 16))

        tk.Label(
            name_row, text=co.name,
            font=Font.LABEL_BOLD, bg=Color.BG_CARD, fg=Color.PRIMARY,
        ).pack(side="left")

        if getattr(co, 'tally_open', False):
            tk.Label(
                name_row, text=" ● Open in Tally",
                font=Font.BODY_SM, bg=Color.SUCCESS_BG, fg=Color.SUCCESS_FG,
                padx=6, pady=2,
            ).pack(side="left", padx=(8, 0))

        # ── Divider ───────────────────────────────────────
        tk.Frame(pad, bg=Color.BORDER, height=1).grid(
            row=2, column=0, columnspan=2, sticky="ew", pady=(0, 14),
        )

        # ── Sync Start Date ───────────────────────────────
        self._add_section_label(pad, row=3, text="Sync Start Date")

        # Pre-fill from Tally books_from or starting_from
        predate = (
            co.books_from    or
            co.starting_from or
            date.today().replace(month=4, day=1).strftime("%Y%m%d")
        )
        try:
            pre_d = datetime.strptime(str(predate)[:8], "%Y%m%d").date()
        except Exception:
            pre_d = date.today().replace(month=4, day=1)

        self._from_var = tk.StringVar(value=_fmt(pre_d))
        self._from_entry = tk.Entry(
            pad, textvariable=self._from_var,
            font=Font.BODY, width=18,
            bg=Color.BG_INPUT, fg=Color.TEXT_PRIMARY,
            relief="solid", bd=1,
        )
        self._from_entry.grid(row=4, column=0, columnspan=2, sticky="w", pady=(4, 2))

        tk.Label(
            pad, text="Format: DD-Mon-YYYY  (e.g. 01-Apr-2024)",
            font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.TEXT_MUTED,
        ).grid(row=5, column=0, columnspan=2, sticky="w")

        # ── Tally Connection ──────────────────────────────
        self._add_section_label(pad, row=6, text="Tally Connection  (leave blank for defaults)")

        host_frame = tk.Frame(pad, bg=Color.BG_CARD)
        host_frame.grid(row=7, column=0, columnspan=2, sticky="w", pady=(4, 0))

        tk.Label(host_frame, text="Host:", font=Font.BODY, bg=Color.BG_CARD,
                 fg=Color.TEXT_SECONDARY, width=6, anchor="w").pack(side="left")
        self._host_var = tk.StringVar(value=co.tally_host or "localhost")
        tk.Entry(
            host_frame, textvariable=self._host_var,
            font=Font.BODY, width=18,
            bg=Color.BG_INPUT, fg=Color.TEXT_PRIMARY,
            relief="solid", bd=1,
        ).pack(side="left", padx=(4, 16))

        tk.Label(host_frame, text="Port:", font=Font.BODY, bg=Color.BG_CARD,
                 fg=Color.TEXT_SECONDARY, width=5, anchor="w").pack(side="left")
        self._port_var = tk.StringVar(value=str(co.tally_port or 9000))
        tk.Entry(
            host_frame, textvariable=self._port_var,
            font=Font.BODY, width=7,
            bg=Color.BG_INPUT, fg=Color.TEXT_PRIMARY,
            relief="solid", bd=1,
        ).pack(side="left", padx=(4, 0))

        # ── Feedback label ────────────────────────────────
        self._feedback = tk.Label(
            pad, text="",
            font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.DANGER,
            wraplength=380, justify="left",
        )
        self._feedback.grid(row=8, column=0, columnspan=2, sticky="w", pady=(12, 0))

        # ── Buttons ───────────────────────────────────────
        btn_row = tk.Frame(pad, bg=Color.BG_CARD)
        btn_row.grid(row=9, column=0, columnspan=2, sticky="ew", pady=(16, 0))
        btn_row.columnconfigure(0, weight=1)

        tk.Button(
            btn_row, text="Cancel",
            font=Font.BUTTON_SM, bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY,
            relief="solid", bd=1, padx=12, pady=5, cursor="hand2",
            command=self.destroy,
        ).pack(side="right", padx=(8, 0))

        tk.Button(
            btn_row, text="✓  Save & Configure",
            font=Font.BUTTON_SM, bg=Color.PRIMARY, fg=Color.TEXT_WHITE,
            relief="flat", bd=0, padx=16, pady=5, cursor="hand2",
            command=self._on_save,
        ).pack(side="right")

    # ─────────────────────────────────────────────────────────────────────────
    def _add_section_label(self, parent, row: int, text: str):
        f = tk.Frame(parent, bg=Color.BG_CARD)
        f.grid(row=row, column=0, columnspan=2, sticky="ew", pady=(14, 0))
        tk.Label(
            f, text=text,
            font=Font.BODY_SM_BOLD, bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY,
        ).pack(side="left")
        tk.Frame(f, bg=Color.BORDER_LIGHT, height=1).pack(
            side="left", fill="x", expand=True, padx=(8, 0),
        )

    # ─────────────────────────────────────────────────────────────────────────
    def _on_save(self):
        co = self._company

        # Validate date
        d = _parse_date(self._from_var.get())
        if not d:
            self._feedback.configure(
                text="Please enter a valid start date  (e.g. 01-Apr-2024)",
            )
            return

        # Validate port
        try:
            port = int(self._port_var.get().strip() or "9000")
        except ValueError:
            self._feedback.configure(text="Port must be a number (e.g. 9000)")
            return

        host          = self._host_var.get().strip() or "localhost"
        starting_from = _yyyymmdd(d)
        books_from    = co.books_from or starting_from

        self._feedback.configure(text="Saving...", fg=Color.TEXT_MUTED)
        self.update_idletasks()

        # Save to DB
        ok, detail = self._app.save_company_to_db(
            company_name  = co.name,
            guid          = co.guid or "",
            starting_from = starting_from,
            books_from    = books_from,
        )

        if not ok:
            self._feedback.configure(
                text=f"✗ Save failed: {detail}", fg=Color.DANGER,
            )
            return

        # Update in-memory state
        co.status        = CompanyStatus.CONFIGURED
        co.starting_from = starting_from
        co.books_from    = books_from
        co.tally_host    = host
        co.tally_port    = port
        self._state.emit("company_updated", name=co.name, company=co)

        self.saved = True
        self.destroy()