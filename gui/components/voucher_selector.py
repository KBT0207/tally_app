"""
gui/components/voucher_selector.py
====================================
Reusable widget: a grid of checkboxes for selecting which voucher types to sync.
Reads/writes to state.voucher_selection (VoucherSelection dataclass).
"""

import tkinter as tk
from gui.styles import Color, Font, Spacing
from gui.state  import VoucherSelection


# Label, attribute name on VoucherSelection, icon
VOUCHER_ITEMS = [
    ("Ledgers",                "ledgers",              "📒"),
    ("Items",                  "items",                "📦"),
    ("Sales",                  "sales",                "🧾"),
    ("Purchase",               "purchase",             "🛒"),
    ("Credit Note",            "credit_note",          "📄"),
    ("Debit Note",             "debit_note",           "📄"),
    ("Receipt",                "receipt",              "💰"),
    ("Payment",                "payment",              "💸"),
    ("Journal",                "journal",              "📓"),
    ("Contra",                 "contra",               "🔄"),
    ("Trial Balance",          "trial_balance",        "⚖️"),
    ("Outstanding Receivable", "outstanding_debtors",  "📊"),
]


class VoucherSelector(tk.Frame):
    """
    Grid of checkboxes — 2 columns.
    Pass in a VoucherSelection object to bind against.
    Changes are reflected in the object immediately.
    """

    def __init__(self, parent, selection: VoucherSelection, **kwargs):
        super().__init__(parent, bg=Color.BG_CARD, **kwargs)
        self._selection = selection
        self._vars: dict[str, tk.BooleanVar] = {}
        self._build()

    def _build(self):
        # Header row
        header = tk.Frame(self, bg=Color.BG_CARD)
        header.grid(row=0, column=0, columnspan=4, sticky="ew", pady=(0, Spacing.SM))

        tk.Label(
            header, text="What to Sync",
            font=Font.LABEL_BOLD, bg=Color.BG_CARD, fg=Color.TEXT_PRIMARY,
        ).pack(side="left")

        self._all_var = tk.BooleanVar(value=self._selection.all_selected())
        tk.Checkbutton(
            header,
            text="Select All",
            variable=self._all_var,
            font=Font.BODY_SM,
            bg=Color.BG_CARD,
            activebackground=Color.BG_CARD,
            fg=Color.TEXT_SECONDARY,
            command=self._toggle_all,
        ).pack(side="right")

        # Checkboxes in 2-column grid
        for idx, (label, attr, icon) in enumerate(VOUCHER_ITEMS):
            row = (idx // 2) + 1
            col = (idx % 2) * 2

            var = tk.BooleanVar(value=getattr(self._selection, attr, True))
            self._vars[attr] = var

            cb = tk.Checkbutton(
                self,
                text=f"{icon}  {label}",
                variable=var,
                font=Font.BODY,
                bg=Color.BG_CARD,
                activebackground=Color.BG_CARD,
                fg=Color.TEXT_PRIMARY,
                anchor="w",
                command=lambda a=attr, v=var: self._on_change(a, v),
            )
            cb.grid(row=row, column=col, columnspan=2, sticky="w",
                    padx=(0, Spacing.XL), pady=2)

    def _on_change(self, attr: str, var: tk.BooleanVar):
        setattr(self._selection, attr, var.get())
        self._all_var.set(self._selection.all_selected())

    def _toggle_all(self):
        val = self._all_var.get()
        for attr, var in self._vars.items():
            var.set(val)
            setattr(self._selection, attr, val)

    def refresh(self):
        """Re-read values from the selection object (e.g. after external change)."""
        for attr, var in self._vars.items():
            var.set(getattr(self._selection, attr, True))
        self._all_var.set(self._selection.all_selected())