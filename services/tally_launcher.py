import os
import time
import subprocess
from typing import Tuple

from logging_config import logger

try:
    import pyautogui
    pyautogui.FAILSAFE = True
    HAS_PYAUTOGUI = True
except ImportError:
    HAS_PYAUTOGUI = False

try:
    import cv2
    HAS_OPENCV = True
except ImportError:
    HAS_OPENCV = False

try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False

try:
    import pygetwindow as gw
    HAS_PYGETWINDOW = True
except ImportError:
    HAS_PYGETWINDOW = False


# Folder where all PNG images are stored (assets/)
ASSETS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "assets")

# Image filenames used for screen detection
IMAGE_FILES = {
    "gateway":     "tally_gateway_screen.png",
    "search_box":  "tally_company_search_box.png",
    "username":    "tally_username_field.png",
    "data_server": "tally_dataserver_image.png",
    "local_path":  "tally_local_path_image.png",
}


class TallyLauncher:

    def __init__(self, state):
        self.state = state

    # ──────────────────────────────────────────────
    #  MAIN ENTRY POINT
    # ──────────────────────────────────────────────

    def prepare(self, company_name: str) -> Tuple[bool, str]:
        """
        Open a company in Tally and wait until it is ready.
        Returns (True, "ready") on success or (False, error_message) on failure.
        """

        if not HAS_PYAUTOGUI:
            return False, "PyAutoGUI not installed"

        # Get company info from state
        company = self.state.companies.get(company_name)
        if not company:
            return False, f"Company '{company_name}' not found"

        tally_exe    = getattr(self.state, 'tally_exe_path', '') or ''
        company_type = getattr(company, 'company_type', 'local') or 'local'

        logger.info(f"[TallyLauncher] Starting: '{company_name}' (type={company_type})")

        try:

            # Step 1 — Close Tally if already open
            if self.is_tally_running():
                logger.info("[TallyLauncher] Tally is open — closing it first...")
                self.kill_tally()
                self.wait_for_tally_to_close()

            # Step 2 — Open Tally
            ok, msg = self.open_tally(tally_exe)
            if not ok:
                return False, msg

            # Step 3 — Handle Tally login screen (if credentials set)
            ok, msg = self.handle_tally_login()
            if not ok:
                return False, msg

            # Step 4 — Wait for the company list screen
            ok, msg = self.wait_for_company_list()
            if not ok:
                return False, msg

            # Step 5 — For TDS: navigate to Data Server first
            if company_type == 'tds':
                ok, msg = self.navigate_to_tds_data_server()
                if not ok:
                    return False, msg

            # Step 6 — Search and open the company
            ok, msg = self.select_company(company)
            if not ok:
                return False, msg

            # Step 7 — Handle company login (username/password if required)
            ok, msg = self.handle_company_login(company, company_type)
            if not ok:
                return False, msg

            # Step 8 — Wait for the Gateway screen (confirms company is open)
            ok, msg = self.wait_for_gateway()
            if not ok:
                return False, f"Gateway not found after opening '{company_name}': {msg}"

            logger.info(f"[TallyLauncher] '{company_name}' is ready ✓")
            return True, "ready"

        except Exception as e:
            logger.exception(f"[TallyLauncher] Unexpected error for '{company_name}'")
            return False, str(e)

    # ──────────────────────────────────────────────
    #  CLOSE TALLY (called after sync is done)
    # ──────────────────────────────────────────────

    def close_tally(self) -> Tuple[bool, str]:
        """Close Tally after sync is done. Safe to call even if Tally is not open."""
        if not self.is_tally_running():
            return True, "not_running"

        logger.info("[TallyLauncher] Closing Tally after sync...")
        ok, msg = self.kill_tally()
        if ok:
            self.wait_for_tally_to_close()
            logger.info("[TallyLauncher] Tally closed ✓")
        return ok, msg

    # ──────────────────────────────────────────────
    #  STEP 1 — CHECK / KILL TALLY
    # ──────────────────────────────────────────────

    def is_tally_running(self) -> bool:
        """Returns True if Tally.exe is currently running."""
        if HAS_PSUTIL:
            for p in psutil.process_iter(['name']):
                if 'tally' in (p.info.get('name') or '').lower():
                    return True
            return False

        # Fallback if psutil not installed
        result = subprocess.run(
            ["tasklist", "/FI", "IMAGENAME eq tally.exe", "/NH"],
            capture_output=True, text=True
        )
        return "tally.exe" in result.stdout.lower()

    def kill_tally(self) -> Tuple[bool, str]:
        """Force-close Tally.exe."""
        logger.info("[TallyLauncher] Killing Tally.exe...")
        subprocess.run(
            ["taskkill", "/F", "/IM", "tally.exe", "/T"],
            capture_output=True, text=True
        )
        logger.info("[TallyLauncher] Tally killed ✓")
        return True, "killed"

    def wait_for_tally_to_close(self):
        """Wait until Tally process is fully gone (max 15 seconds)."""
        if not HAS_PSUTIL:
            time.sleep(5)
            return

        for _ in range(15):
            still_running = False
            for p in psutil.process_iter(['name']):
                if 'tally' in (p.info.get('name') or '').lower():
                    still_running = True
                    break
            if not still_running:
                logger.info("[TallyLauncher] Tally process closed ✓")
                time.sleep(2)  # extra wait for Windows to release file locks
                return
            time.sleep(1)

        logger.warning("[TallyLauncher] Tally still running after 15s — continuing anyway")
        time.sleep(3)

    # ──────────────────────────────────────────────
    #  STEP 2 — OPEN TALLY
    # ──────────────────────────────────────────────

    def open_tally(self, exe_path: str) -> Tuple[bool, str]:
        """Launch Tally.exe and wait for its window to appear."""
        if not exe_path:
            return False, "Tally.exe path not set. Go to Settings → Automation."
        if not os.path.exists(exe_path):
            return False, f"Tally.exe not found at: {exe_path}"

        logger.info(f"[TallyLauncher] Opening Tally: {exe_path}")
        subprocess.Popen([exe_path])

        # Wait up to 30 seconds for the Tally window to appear
        logger.info("[TallyLauncher] Waiting for Tally window...")
        for _ in range(30):
            if self.get_tally_window() is not None:
                logger.info("[TallyLauncher] Tally window appeared ✓")
                break
            time.sleep(1)

        # Wait extra time for Tally to fully load
        logger.info("[TallyLauncher] Waiting 6s for Tally to load...")
        time.sleep(6)
        return True, "launched"

    # ──────────────────────────────────────────────
    #  STEP 3 — TALLY LOGIN (Tally-level password)
    # ──────────────────────────────────────────────

    def handle_tally_login(self) -> Tuple[bool, str]:
        """
        If Tally has a login screen (Tally-level username/password),
        type the credentials and press Enter.
        If no credentials are configured, skip this step.
        """
        username = getattr(self.state, 'tally_username', '') or ''
        password = getattr(self.state, 'tally_password', '') or ''

        if not username:
            logger.info("[TallyLauncher] No Tally login credentials — skipping")
            return True, "skipped"

        logger.info("[TallyLauncher] Checking for Tally login screen...")
        found = self.wait_for_image("username", seconds=8)

        if not found:
            logger.info("[TallyLauncher] No Tally login screen — moving on")
            return True, "no_login_screen"

        logger.info("[TallyLauncher] Tally login screen found — entering credentials")
        self.bring_tally_to_front()
        time.sleep(1)

        # Tally auto-focuses the username field
        self.type_text(username)
        time.sleep(0.3)
        pyautogui.press('enter')
        time.sleep(1)

        self.type_text(password)
        time.sleep(0.3)
        pyautogui.press('enter')
        time.sleep(2)

        logger.info("[TallyLauncher] Tally login submitted ✓")
        return True, "logged_in"

    # ──────────────────────────────────────────────
    #  STEP 4 — WAIT FOR COMPANY LIST
    # ──────────────────────────────────────────────

    def wait_for_company_list(self) -> Tuple[bool, str]:
        """Wait for the Select Company screen (the yellow search box)."""
        logger.info("[TallyLauncher] Waiting for company list screen...")
        found = self.wait_for_image("search_box", seconds=self.get_timeout())
        if found:
            logger.info("[TallyLauncher] Company list screen ready ✓")
            time.sleep(1)
            return True, "ready"
        return False, "Company list screen did not appear"

    # ──────────────────────────────────────────────
    #  STEP 5 — TDS: NAVIGATE TO DATA SERVER
    # ──────────────────────────────────────────────

    def navigate_to_tds_data_server(self) -> Tuple[bool, str]:
        """
        TDS only: double-click the Data Server button,
        then wait for the local path screen,
        then wait for the company list to reload.
        """
        logger.info("[TallyLauncher] TDS: Navigating to Data Server...")

        self.bring_tally_to_front()
        time.sleep(0.5)

        # Double-click the Data Server button
        ok = self.double_click_image("data_server")
        if not ok:
            return False, "Data Server button not found on screen"

        logger.info("[TallyLauncher] TDS: Data Server clicked ✓ — waiting for path screen...")

        # Move mouse away so it doesn't block image detection
        pyautogui.moveTo(5, pyautogui.size().height // 2)
        time.sleep(0.5)

        # Wait for local path screen to appear
        found = self.wait_for_image("local_path", seconds=self.get_timeout())
        if not found:
            return False, "Local path screen did not appear after clicking Data Server"

        logger.info("[TallyLauncher] TDS: Local path screen found ✓ — waiting for company list...")

        # Move mouse away again
        pyautogui.moveTo(5, pyautogui.size().height // 2)

        # Wait for company list (search box) to reload
        found = self.wait_for_image("search_box", seconds=self.get_timeout())
        if not found:
            return False, "Company list did not reload after TDS navigation"

        logger.info("[TallyLauncher] TDS: Company list ready ✓")
        time.sleep(0.5)
        return True, "tds_ready"

    # ──────────────────────────────────────────────
    #  STEP 6 — SEARCH AND OPEN COMPANY
    # ──────────────────────────────────────────────

    def select_company(self, company) -> Tuple[bool, str]:
        """Click the search box, type the company name, press Enter."""
        self.bring_tally_to_front()
        time.sleep(0.5)

        name = str(company.name or '').strip()
        logger.info(f"[TallyLauncher] Searching for company: '{name}'")

        # Click the search box
        clicked = self.click_image("search_box")
        if not clicked:
            logger.warning("[TallyLauncher] Search box not found — typing anyway")
        time.sleep(0.5)

        # Type the company name slowly so Tally's filter can keep up
        self.type_text(name)
        time.sleep(1)

        # Press Enter to open the company
        logger.info("[TallyLauncher] Pressing Enter to open company")
        pyautogui.press('enter')
        time.sleep(1.5)

        return True, "company_selected"

    # ──────────────────────────────────────────────
    #  STEP 7 — COMPANY LOGIN (per-company password)
    # ──────────────────────────────────────────────

    def handle_company_login(self, company, company_type: str) -> Tuple[bool, str]:
        """
        Type username and password for the company login dialog.

        LOCAL: wait for username image to appear, then type credentials.
        TDS:   skip image detection — just wait 3 seconds then type credentials.
               (TDS search box can cause false image matches, so we skip detection.)
        """
        username = getattr(company, 'tally_username', '') or ''
        password = getattr(company, 'tally_password', '') or ''

        # ── TDS login ──────────────────────────────────────────────────────
        if company_type == 'tds':
            if not username:
                logger.info("[TallyLauncher] TDS: No company credentials — skipping login")
                return True, "no_login"

            logger.info("[TallyLauncher] TDS: Waiting 3s for login dialog...")
            time.sleep(3)

            self.bring_tally_to_front()
            time.sleep(0.5)

            logger.info(f"[TallyLauncher] TDS: Typing username '{username}'")
            self.type_text(username)
            time.sleep(0.3)
            pyautogui.press('enter')
            time.sleep(1)

            self.type_text(password)
            time.sleep(0.3)
            pyautogui.press('enter')
            time.sleep(2)

            logger.info("[TallyLauncher] TDS: Login submitted ✓")
            return True, "logged_in"

        # ── Local / Remote login ───────────────────────────────────────────
        logger.info("[TallyLauncher] Waiting for company login dialog (10s)...")
        found = self.wait_for_image("username", seconds=10)

        if not found:
            logger.info("[TallyLauncher] No login dialog — company opened directly ✓")
            return True, "no_login"

        logger.info(f"[TallyLauncher] Login dialog found — typing username '{username}'")
        self.bring_tally_to_front()
        time.sleep(1)

        self.type_text(username)
        time.sleep(0.3)
        pyautogui.press('enter')
        time.sleep(1)

        self.type_text(password)
        time.sleep(0.3)
        pyautogui.press('enter')
        time.sleep(2)

        # Check if dialog is gone (login success)
        img_path = os.path.join(ASSETS_DIR, IMAGE_FILES["username"])
        for _ in range(8):
            if not self.find_image_on_screen(img_path):
                logger.info("[TallyLauncher] Login successful ✓")
                return True, "logged_in"
            time.sleep(1)

        return False, (
            f"Login failed for '{company.name}' — dialog still visible. "
            "Check username/password in Configure Company."
        )

    # ──────────────────────────────────────────────
    #  STEP 8 — WAIT FOR GATEWAY
    # ──────────────────────────────────────────────

    def wait_for_gateway(self) -> Tuple[bool, str]:
        """Wait for the Tally Gateway screen — confirms company is fully open."""
        timeout = self.get_timeout()
        logger.info(f"[TallyLauncher] Waiting for Gateway screen (timeout={timeout}s)...")

        found = self.wait_for_image("gateway", seconds=timeout)
        if found:
            logger.info("[TallyLauncher] Gateway screen found ✓")
            return True, "gateway_found"

        # Fallback: try connecting via XML to confirm Tally is responding
        logger.warning("[TallyLauncher] Gateway image not found — trying XML connection...")
        for attempt in range(5):
            try:
                from services.tally_connector import TallyConnector
                tc = TallyConnector(host=self.state.tally.host, port=self.state.tally.port)
                if tc.status == "Connected":
                    logger.info("[TallyLauncher] Gateway confirmed via XML ✓")
                    return True, "gateway_xml"
            except Exception as e:
                logger.debug(f"[TallyLauncher] XML attempt {attempt + 1}: {e}")
            time.sleep(3)

        return False, "Gateway not found — image not detected and XML not responding"

    # ──────────────────────────────────────────────
    #  HELPER — WINDOW
    # ──────────────────────────────────────────────

    def get_tally_window(self):
        """Get the Tally window object. Returns None if not found."""
        if not HAS_PYGETWINDOW:
            return None
        wins = gw.getWindowsWithTitle("Tally") or gw.getWindowsWithTitle("tally")
        return wins[0] if wins else None

    def bring_tally_to_front(self):
        """Bring the Tally window to the front so we can type into it."""
        if not HAS_PYGETWINDOW:
            time.sleep(0.3)
            return
        win = self.get_tally_window()
        if not win:
            return
        try:
            if win.isMinimized:
                win.restore()
                time.sleep(0.5)
            win.activate()
            time.sleep(0.8)
        except Exception:
            pass

    # ──────────────────────────────────────────────
    #  HELPER — IMAGE DETECTION
    # ──────────────────────────────────────────────

    def find_image_on_screen(self, img_path: str):
        """
        Look for an image on the screen.
        Returns the location if found, or None if not found.
        """
        try:
            if HAS_OPENCV:
                return pyautogui.locateOnScreen(img_path, confidence=self.get_confidence(), grayscale=True)
            else:
                return pyautogui.locateOnScreen(img_path)
        except pyautogui.ImageNotFoundException:
            return None
        except Exception:
            return None

    def wait_for_image(self, image_key: str, seconds: int = 30) -> bool:
        """
        Wait up to `seconds` for an image to appear on screen.
        Returns True if found, False if timed out.
        image_key is a key from IMAGE_FILES dict (e.g. "search_box", "gateway").
        """
        # Get image filename from state (loaded from DB) or fall back to defaults
        images   = getattr(self.state, 'tally_images', {}) or {}
        filename = images.get(image_key) or IMAGE_FILES.get(image_key) or f"tally_{image_key}.png"
        img_path = os.path.join(ASSETS_DIR, filename)

        if not os.path.exists(img_path):
            logger.warning(f"[TallyLauncher] Image file not found: {filename}")
            return False

        end_time = time.time() + seconds
        while time.time() < end_time:
            if self.find_image_on_screen(img_path):
                return True
            time.sleep(1)

        logger.warning(f"[TallyLauncher] Timed out waiting for: {filename}")
        return False

    def click_image(self, image_key: str) -> bool:
        """
        Find an image on screen and click it.
        Returns True if clicked, False if image not found.
        """
        images   = getattr(self.state, 'tally_images', {}) or {}
        filename = images.get(image_key) or IMAGE_FILES.get(image_key) or f"tally_{image_key}.png"
        img_path = os.path.join(ASSETS_DIR, filename)

        if not os.path.exists(img_path):
            logger.warning(f"[TallyLauncher] Image file not found: {filename}")
            return False

        for _ in range(3):
            loc = self.find_image_on_screen(img_path)
            if loc:
                x, y = pyautogui.center(loc)
                pyautogui.click(x, y)
                return True
            time.sleep(1)

        logger.warning(f"[TallyLauncher] Could not find image to click: {filename}")
        return False

    def double_click_image(self, image_key: str) -> bool:
        """
        Find an image on screen and double-click it.
        Returns True if clicked, False if image not found.
        """
        images   = getattr(self.state, 'tally_images', {}) or {}
        filename = images.get(image_key) or IMAGE_FILES.get(image_key) or f"tally_{image_key}.png"
        img_path = os.path.join(ASSETS_DIR, filename)

        if not os.path.exists(img_path):
            logger.warning(f"[TallyLauncher] Image file not found: {filename}")
            return False

        for _ in range(3):
            loc = self.find_image_on_screen(img_path)
            if loc:
                x, y = pyautogui.center(loc)
                pyautogui.doubleClick(x, y)
                return True
            time.sleep(1)

        logger.warning(f"[TallyLauncher] Could not find image to double-click: {filename}")
        return False

    # ──────────────────────────────────────────────
    #  HELPER — TYPING
    # ──────────────────────────────────────────────

    def type_text(self, text) -> None:
        """Type text character by character into the active window."""
        if not text:
            return
        pyautogui.write(str(text), interval=0.1)

    # ──────────────────────────────────────────────
    #  HELPER — SETTINGS FROM STATE
    # ──────────────────────────────────────────────

    def get_confidence(self) -> float:
        """Image match confidence (0.0 to 1.0). Default 0.80."""
        aut = getattr(self.state, 'automation', None)
        return float(getattr(aut, 'confidence', 0.80)) if aut else 0.80

    def get_timeout(self) -> int:
        """How many seconds to wait for images. Default 30."""
        aut = getattr(self.state, 'automation', None)
        return int(getattr(aut, 'wait_timeout_sec', 30)) if aut else 30