"""
gui/scale.py  —  Windows DPI scaling utility for TallySyncManager.

STANDALONE — does NOT modify Font, Spacing, or Layout in styles.py.
The original styles.py stays 100% unchanged.

This file only needs to exist. Nothing imports it yet.
It is ready for the NEXT phase when you want true DPI scaling.
"""

import ctypes
import tkinter as tk


class _Scale:
    def __init__(self):
        self._factor = 1.0
        self._sw = 1920
        self._sh = 1080
        self._ready = False

    def init(self, root: tk.Tk):
        if self._ready:
            return
        try:
            ctypes.windll.shcore.SetProcessDpiAwareness(2)
        except Exception:
            try:
                ctypes.windll.user32.SetProcessDPIAware()
            except Exception:
                pass
        self._sw = root.winfo_screenwidth()
        self._sh = root.winfo_screenheight()
        try:
            dpi = ctypes.windll.user32.GetDpiForSystem()
            self._factor = round(dpi / 96.0, 3)
        except Exception:
            try:
                self._factor = round(root.winfo_fpixels("1i") / 96.0, 3)
            except Exception:
                self._factor = 1.0
        self._factor = max(1.0, min(self._factor, 3.0))
        self._ready = True

    @property
    def factor(self): return self._factor
    @property
    def screen_w(self): return self._sw
    @property
    def screen_h(self): return self._sh

    def px(self, base: int) -> int:
        return max(1, int(round(base * self._factor)))

    def font(self, base: int) -> int:
        return max(6, int(round(base * self._factor)))

    def sidebar_width(self) -> int:
        base = 220 if self._sw >= 2560 else 210 if self._sw >= 1920 else 180 if self._sw <= 1366 else 200
        return self.px(base)

    def header_height(self) -> int:
        return self.px(60)

    def card_height(self) -> int:
        return self.px(72)

    def min_window_size(self):
        w = min(self.px(1100), int(self._sw * 0.92))
        h = min(self.px(680),  int(self._sh * 0.88))
        return w, h

    def startup_geometry(self, root: tk.Tk):
        w = min(int(self._sw * 0.80), self.px(1400))
        h = min(int(self._sh * 0.85), self.px(900))
        min_w, min_h = self.min_window_size()
        w, h = max(w, min_w), max(h, min_h)
        x = max(0, (self._sw - w) // 2)
        y = max(0, (self._sh - h) // 2)
        root.geometry(f"{w}x{h}+{x}+{y}")
        root.minsize(min_w, min_h)

    def place_dialog(self, dialog: tk.Toplevel, base_w: int, base_h: int, parent=None):
        dialog.update_idletasks()
        w = min(self.px(base_w), int(self._sw * 0.95))
        h = min(self.px(base_h), int(self._sh * 0.92))
        if parent:
            try:
                cx = parent.winfo_rootx() + parent.winfo_width()  // 2
                cy = parent.winfo_rooty() + parent.winfo_height() // 2
            except Exception:
                cx, cy = self._sw // 2, self._sh // 2
        else:
            cx, cy = self._sw // 2, self._sh // 2
        x = max(8, min(cx - w // 2, self._sw - w - 8))
        y = max(8, min(cy - h // 2, self._sh - h - 8))
        dialog.geometry(f"{w}x{h}+{x}+{y}")


S = _Scale()