"""
gui/components/image_test_overlay.py
======================================
Transparent overlay window that draws a red rectangle around
a PyAutoGUI-found image on screen.

Usage:
    overlay = ImageTestOverlay(root, x, y, w, h)
    # Auto-closes after 2 seconds

How it works:
  - Creates a borderless, always-on-top Toplevel window
  - Positioned exactly over the found image region
  - Draws a thick red rectangle inside a transparent canvas
  - Auto-destroys after 2 seconds
  - Works on Windows (wm_attributes -transparentcolor)
"""

import tkinter as tk


class ImageTestOverlay(tk.Toplevel):
    """
    Draws a red rectangle on screen at the given coordinates.
    Auto-closes after `duration_ms` milliseconds.

    Parameters:
        parent        — root Tk window
        x, y          — top-left corner of found region (screen coords)
        w, h          — width and height of found region
        duration_ms   — how long to show overlay (default 2000ms)
    """

    BORDER_COLOR    = "#FF0000"   # Red
    BORDER_WIDTH    = 4           # pixels
    FILL_COLOR      = "#FF0000"   # Same red (will be made transparent)
    TRANSPARENT_KEY = "#010101"   # Near-black — used as transparent color on Windows

    def __init__(
        self,
        parent,
        x: int, y: int,
        w: int, h: int,
        duration_ms: int = 2000,
    ):
        super().__init__(parent)

        self._x    = x
        self._y    = y
        self._w    = max(w, 20)
        self._h    = max(h, 20)

        # ── Window setup ──────────────────────────────────────────────────────
        self.overrideredirect(True)        # No title bar, no borders
        self.attributes("-topmost", True)  # Always on top of everything

        # Position exactly over the found image
        self.geometry(f"{self._w}x{self._h}+{self._x}+{self._y}")

        # ── Transparency (Windows) ────────────────────────────────────────────
        # On Windows: set a "transparent color" so the fill disappears,
        # leaving only the red border visible.
        try:
            self.attributes("-transparentcolor", self.TRANSPARENT_KEY)
            fill = self.TRANSPARENT_KEY   # Interior will be invisible
        except Exception:
            # Non-Windows: use semi-transparent alpha instead
            try:
                self.attributes("-alpha", 0.4)
            except Exception:
                pass
            fill = ""   # Let canvas default fill show

        # ── Canvas with red border rectangle ─────────────────────────────────
        canvas = tk.Canvas(
            self,
            width             = self._w,
            height            = self._h,
            bg                = self.TRANSPARENT_KEY,
            highlightthickness = 0,
            bd                = 0,
        )
        canvas.pack(fill="both", expand=True)

        # Draw red rectangle border
        b = self.BORDER_WIDTH
        canvas.create_rectangle(
            b, b,
            self._w - b, self._h - b,
            outline = self.BORDER_COLOR,
            width   = self.BORDER_WIDTH * 2,
            fill    = fill,
        )

        # ── Auto-close ────────────────────────────────────────────────────────
        self.after(duration_ms, self._close)

    def _close(self):
        try:
            self.destroy()
        except Exception:
            pass


def show_found_overlay(parent, location, duration_ms: int = 2000):
    """
    Convenience function — show red rectangle at a PyAutoGUI location result.

    location: pyautogui.Box namedtuple with (left, top, width, height)
              or any object with .left, .top, .width, .height attributes
    """
    try:
        x = int(location.left)
        y = int(location.top)
        w = int(location.width)
        h = int(location.height)
        return ImageTestOverlay(parent, x, y, w, h, duration_ms)
    except Exception as e:
        print(f"[ImageTestOverlay] Could not show overlay: {e}")
        return None