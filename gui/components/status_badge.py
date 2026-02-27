"""
gui/components/status_badge.py
================================
A small pill-shaped label that shows company sync status.
Reads color/icon from styles.STATUS_STYLE.
"""

import tkinter as tk
from gui.styles import Color, Font, STATUS_STYLE


class StatusBadge(tk.Frame):
    """
    Small colored badge like:  ● Configured   ✓ Sync Done   ✗ Error
    """

    def __init__(self, parent, status: str = "Not Configured", **kwargs):
        style = STATUS_STYLE.get(status, STATUS_STYLE["Not Configured"])
        super().__init__(
            parent,
            bg=style["bg"],
            padx=7,
            pady=2,
            **kwargs,
        )
        self._lbl = tk.Label(
            self,
            text=f"{style['icon']}  {status}",
            font=Font.BADGE,
            bg=style["bg"],
            fg=style["fg"],
        )
        self._lbl.pack()

    def set_status(self, status: str):
        style = STATUS_STYLE.get(status, STATUS_STYLE["Not Configured"])
        self.configure(bg=style["bg"])
        self._lbl.configure(
            text=f"{style['icon']}  {status}",
            bg=style["bg"],
            fg=style["fg"],
        )