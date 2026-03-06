"""
gui/tray_manager.py
====================
System tray icon for TallySync.

Behaviour:
  - Clicking ✖ on the main window HIDES it (does not quit)
  - App keeps running in the system tray
  - Tray icon right-click menu:
      ├── Open TallySync          ← brings window back
      ├── ── status line ──       ← shows how many companies scheduled
      ├── Pause All Syncs         ← toggle
      ├── ────────────────
      └── Exit TallySync          ← truly quits the app

Dependencies:
    pip install pystray pillow

If pystray is not installed the tray is silently skipped and the app
behaves exactly as before (✖ still closes).

Usage (from app.py):
    from gui.tray_manager import TrayManager
    self._tray = TrayManager(
        root             = self.root,
        state            = self.state,
        on_open          = self._show_window,
        on_pause_toggle  = self._toggle_pause,
        on_exit          = self._quit_app,
    )
    self._tray.start()           # call once after UI is built
    self._tray.update_tooltip()  # call whenever company list changes
"""

import threading
from typing import Callable, Optional

# ── Try to import pystray + PIL ───────────────────────────────────────────────
try:
    import pystray
    from pystray import MenuItem as TrayItem, Menu as TrayMenu
    HAS_PYSTRAY = True
except ImportError:
    HAS_PYSTRAY = False

try:
    from PIL import Image, ImageDraw
    HAS_PIL = True
except ImportError:
    HAS_PIL = False

HAS_TRAY = HAS_PYSTRAY and HAS_PIL


def _make_icon_image(size: int = 64) -> "Image.Image":
    """
    Generate a simple icon image programmatically.
    A blue circle with a white lightning bolt — no external .ico needed.
    Falls back gracefully if PIL draw fails.
    """
    img  = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    # Blue circle background
    draw.ellipse([2, 2, size - 2, size - 2], fill="#2C3E7A")

    # White lightning bolt (⚡) approximated as a polygon
    cx, cy = size // 2, size // 2
    bolt = [
        (cx + 4,  cy - 18),
        (cx - 4,  cy - 2),
        (cx + 4,  cy - 2),
        (cx - 6,  cy + 18),
        (cx + 8,  cy + 2),
        (cx - 2,  cy + 2),
    ]
    draw.polygon(bolt, fill="#FFFFFF")
    return img


class TrayManager:
    """
    Manages the system tray icon lifecycle.

    Parameters
    ----------
    root            : tk.Tk  — the main Tkinter root window
    state           : AppState
    on_open         : callable — show/raise the main window
    on_pause_toggle : callable — toggle pause state, returns new bool (True=paused)
    on_exit         : callable — cleanly shut down and quit
    """

    def __init__(
        self,
        root,
        state,
        on_open:         Callable,
        on_pause_toggle: Callable,
        on_exit:         Callable,
    ):
        self._root            = root
        self._state           = state
        self._on_open         = on_open
        self._on_pause_toggle = on_pause_toggle
        self._on_exit         = on_exit
        self._icon: Optional["pystray.Icon"] = None
        self._paused          = False
        self._available       = HAS_TRAY

    # ─────────────────────────────────────────────────────────────────────────
    #  Public API
    # ─────────────────────────────────────────────────────────────────────────
    @property
    def available(self) -> bool:
        """True if pystray + PIL are installed and tray is usable."""
        return self._available

    def start(self):
        """
        Build the tray icon and run it in a daemon thread.
        Safe to call even if pystray is not installed — silently does nothing.
        """
        if not self._available:
            return

        icon_image = _make_icon_image()
        self._icon = pystray.Icon(
            name    = "TallySync",
            icon    = icon_image,
            title   = self._tooltip_text(),
            menu    = self._build_menu(),
        )

        # Run in daemon thread so it doesn't block app exit
        t = threading.Thread(
            target = self._icon.run,
            daemon = True,
            name   = "TrayIconThread",
        )
        t.start()

    def update_tooltip(self):
        """
        Refresh the tray tooltip text (e.g. after companies are loaded).
        Call this whenever the company list or schedule changes.
        """
        if self._icon:
            self._icon.title = self._tooltip_text()

    def hide_to_tray(self):
        """Hide the main window to tray. Called when user clicks ✖."""
        if not self._available:
            return   # No tray — caller should handle normal close
        self._root.withdraw()
        if self._icon:
            self._icon.notify(
                title   = "TallySync is still running",
                message = "Syncs will continue in the background.\n"
                          "Right-click the tray icon to open or exit.",
            )

    def show_window(self):
        """Bring the main window back from the tray."""
        self._root.deiconify()
        self._root.lift()
        self._root.focus_force()

    def stop(self):
        """Remove the tray icon. Call on true app exit."""
        if self._icon:
            try:
                self._icon.stop()
            except Exception:
                pass

    # ─────────────────────────────────────────────────────────────────────────
    #  Internal helpers
    # ─────────────────────────────────────────────────────────────────────────
    def _tooltip_text(self) -> str:
        """Build tray tooltip: 'TallySync • 3 companies scheduled'"""
        try:
            scheduled = sum(
                1 for co in self._state.companies.values()
                if getattr(co, "schedule_enabled", False)
            )
            if scheduled == 0:
                return "TallySync • No companies scheduled"
            return f"TallySync • {scheduled} company scheduled" \
                   if scheduled == 1 \
                   else f"TallySync • {scheduled} companies scheduled"
        except Exception:
            return "TallySync"

    def _build_menu(self) -> "TrayMenu":
        """Build the right-click tray menu."""
        return TrayMenu(
            TrayItem("Open TallySync",      self._action_open, default=True),
            TrayMenu.SEPARATOR,
            TrayItem(
                lambda item: "▶ Resume All Syncs" if self._paused else "⏸ Pause All Syncs",
                self._action_pause_toggle,
            ),
            TrayMenu.SEPARATOR,
            TrayItem("Exit TallySync",      self._action_exit),
        )

    # ── Menu action handlers (called from tray thread — must be thread-safe) ─
    def _action_open(self, icon, item):
        """Open/raise main window — must run on Tkinter main thread."""
        self._root.after(0, self._on_open)

    def _action_pause_toggle(self, icon, item):
        """Toggle pause — runs on Tkinter main thread."""
        def _do():
            self._paused = self._on_pause_toggle()
            # Rebuild menu so label updates
            if self._icon:
                self._icon.menu = self._build_menu()
                self._icon.update_menu()
        self._root.after(0, _do)

    def _action_exit(self, icon, item):
        """Truly exit the app — runs on Tkinter main thread."""
        self._root.after(0, self._on_exit)