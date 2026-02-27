"""
gui/components/date_range_picker.py
=====================================
From/To date picker with calendar popup (tkcalendar).
Falls back to a plain Entry field if tkcalendar is not installed.
Returns dates as YYYYMMDD strings (matching your sync_service format).
"""

import tkinter as tk
from tkinter import messagebox
from datetime import datetime, date

from gui.styles import Color, Font, Spacing

try:
    import ttkbootstrap  # noqa: F401
    _HAS_TTKBOOTSTRAP = True
except ImportError:
    _HAS_TTKBOOTSTRAP = False

try:
    from tkcalendar import DateEntry
    # tkcalendar's DateEntry is incompatible with ttkbootstrap — it crashes
    # during __init__ because ttkbootstrap patches ttk.Entry.__init__ to call
    # configure(style=...) before tkcalendar has set up the _calendar attribute.
    HAS_CALENDAR = not _HAS_TTKBOOTSTRAP
except ImportError:
    HAS_CALENDAR = False


def _parse_yyyymmdd(s: str):
    """Parse YYYYMMDD string → date object. Returns None on failure."""
    try:
        return datetime.strptime(str(s)[:8], "%Y%m%d").date()
    except Exception:
        return None


def _fmt_display(d: date) -> str:
    return d.strftime("%d-%b-%Y") if d else ""


def _to_yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d") if d else ""


class DateRangePicker(tk.Frame):
    """
    Two date fields: From and To.
    from_date / to_date properties return YYYYMMDD strings.
    """

    def __init__(
        self,
        parent,
        from_date: str = None,   # YYYYMMDD
        to_date:   str = None,   # YYYYMMDD
        label:     str = "Date Range",
        **kwargs,
    ):
        super().__init__(parent, bg=Color.BG_CARD, **kwargs)
        self._label    = label
        self._from_var = tk.StringVar()
        self._to_var   = tk.StringVar()

        # Default to_date = today
        today = date.today()
        self._to_d   = today
        self._from_d = _parse_yyyymmdd(from_date) if from_date else date(today.year, 4, 1)

        self._build()
        self._set_display()

    def _build(self):
        tk.Label(
            self, text=self._label,
            font=Font.LABEL_BOLD, bg=Color.BG_CARD, fg=Color.TEXT_PRIMARY,
        ).grid(row=0, column=0, columnspan=4, sticky="w", pady=(0, Spacing.SM))

        # From
        tk.Label(self, text="From:", font=Font.BODY,
                 bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY,
                 width=5, anchor="w").grid(row=1, column=0, sticky="w")

        if HAS_CALENDAR:
            self._from_entry = DateEntry(
                self, width=13, background=Color.PRIMARY,
                foreground="white", borderwidth=2,
                date_pattern="dd-MMM-yyyy",
                font=Font.BODY,
            )
            self._from_entry.grid(row=1, column=1, sticky="w", padx=(Spacing.SM, Spacing.XL))
            if self._from_d:
                self._from_entry.set_date(self._from_d)
            self._from_entry.bind("<<DateEntrySelected>>", self._on_from_change)
        else:
            self._from_entry = tk.Entry(
                self, textvariable=self._from_var,
                font=Font.BODY, width=14,
                bg=Color.BG_INPUT, fg=Color.TEXT_PRIMARY,
                relief="solid", bd=1,
            )
            self._from_entry.grid(row=1, column=1, sticky="w", padx=(Spacing.SM, Spacing.XL))
            self._from_var.set(_fmt_display(self._from_d))

        # To
        tk.Label(self, text="To:", font=Font.BODY,
                 bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY,
                 width=3, anchor="w").grid(row=1, column=2, sticky="w")

        if HAS_CALENDAR:
            self._to_entry = DateEntry(
                self, width=13, background=Color.PRIMARY,
                foreground="white", borderwidth=2,
                date_pattern="dd-MMM-yyyy",
                font=Font.BODY,
            )
            self._to_entry.grid(row=1, column=3, sticky="w", padx=(Spacing.SM, 0))
            self._to_entry.set_date(self._to_d)
            self._to_entry.bind("<<DateEntrySelected>>", self._on_to_change)
        else:
            self._to_entry = tk.Entry(
                self, textvariable=self._to_var,
                font=Font.BODY, width=14,
                bg=Color.BG_INPUT, fg=Color.TEXT_PRIMARY,
                relief="solid", bd=1,
            )
            self._to_entry.grid(row=1, column=3, sticky="w", padx=(Spacing.SM, 0))
            self._to_var.set(_fmt_display(self._to_d))

        if not HAS_CALENDAR:
            tk.Label(
                self, text="Format: DD-Mon-YYYY  e.g. 01-Apr-2024",
                font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.TEXT_MUTED,
            ).grid(row=2, column=0, columnspan=4, sticky="w", pady=(4, 0))

    def _set_display(self):
        if not HAS_CALENDAR:
            self._from_var.set(_fmt_display(self._from_d))
            self._to_var.set(_fmt_display(self._to_d))

    def _on_from_change(self, e=None):
        if HAS_CALENDAR:
            self._from_d = self._from_entry.get_date()

    def _on_to_change(self, e=None):
        if HAS_CALENDAR:
            self._to_d = self._to_entry.get_date()

    def _parse_manual(self, s: str) -> date:
        for fmt in ("%d-%b-%Y", "%d/%m/%Y", "%Y-%m-%d", "%Y%m%d", "%d-%m-%Y"):
            try:
                return datetime.strptime(s.strip(), fmt).date()
            except ValueError:
                continue
        return None

    # ── Public API ────────────────────────────────────────────────────────────
    @property
    def from_date(self) -> str:
        """Returns YYYYMMDD string."""
        if HAS_CALENDAR:
            self._from_d = self._from_entry.get_date()
        else:
            d = self._parse_manual(self._from_var.get())
            if d:
                self._from_d = d
        return _to_yyyymmdd(self._from_d) if self._from_d else ""

    @property
    def to_date(self) -> str:
        """Returns YYYYMMDD string."""
        if HAS_CALENDAR:
            self._to_d = self._to_entry.get_date()
        else:
            d = self._parse_manual(self._to_var.get())
            if d:
                self._to_d = d
        return _to_yyyymmdd(self._to_d) if self._to_d else ""

    def set_from_date(self, yyyymmdd: str):
        d = _parse_yyyymmdd(yyyymmdd)
        if not d:
            return
        self._from_d = d
        if HAS_CALENDAR:
            self._from_entry.set_date(d)
        else:
            self._from_var.set(_fmt_display(d))

    def set_to_date(self, yyyymmdd: str):
        d = _parse_yyyymmdd(yyyymmdd)
        if not d:
            return
        self._to_d = d
        if HAS_CALENDAR:
            self._to_entry.set_date(d)
        else:
            self._to_var.set(_fmt_display(d))

    def validate(self) -> tuple[bool, str]:
        """Returns (is_valid, error_message)."""
        fd = self.from_date
        td = self.to_date
        if not fd:
            return False, "Please enter a valid From date."
        if not td:
            return False, "Please enter a valid To date."
        if fd > td:
            return False, "From date cannot be after To date."
        return True, ""