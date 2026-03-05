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
    import cv2  # noqa
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

try:
    import pyperclip
    HAS_PYPERCLIP = True
except ImportError:
    HAS_PYPERCLIP = False

_ASSETS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "assets")


class TallyLaunchError(Exception):
    pass


class TallyLauncher:

    def __init__(self, state):
        self._state = state

    def prepare(self, company_name: str) -> Tuple[bool, str]:

        if not HAS_PYAUTOGUI:
            return False, "PyAutoGUI not installed"

        co = self._state.companies.get(company_name)
        if not co:
            return False, f"Company '{company_name}' not found in state"

        exe_path = getattr(self._state, 'tally_exe_path', '') or ''
        logger.info(f"[TallyLauncher] Preparing: '{company_name}'")

        try:

            if self._is_tally_running():
                logger.info("[TallyLauncher] Tally running — checking open companies...")
                open_companies = self._get_all_open_companies()
                target = company_name.strip().lower()
                open_lower = [n.strip().lower() for n in open_companies]

                if target in open_lower and len(open_companies) == 1:
                    # Company is already open in Tally.
                    # XML already confirmed it's responsive (we just called _get_all_open_companies).
                    # No need to wait for gateway image — skip straight to ready.
                    logger.info(
                        f"[TallyLauncher] '{company_name}' already open and Tally responding via XML ✓ — skipping gateway wait"
                    )
                    return True, "ready"

                logger.info(f"[TallyLauncher] Need to switch. Open: {open_companies} → killing Tally")
                ok, msg = self._kill_tally()
                if not ok:
                    return False, f"Could not kill Tally: {msg}"
                self._wait_for_tally_exit()

            else:
                logger.info("[TallyLauncher] Tally not running")

            ok, msg = self._launch_tally(exe_path)
            if not ok:
                return False, msg

            ok, msg = self._handle_tally_login()
            if not ok:
                return False, msg

            ok, msg = self._wait_for_select_company_screen()
            if not ok:
                return False, msg

            ok, msg = self._set_path_if_needed(co)
            if not ok:
                return False, msg

            ok, msg = self._select_company(co)
            if not ok:
                return False, msg

            ok, msg = self._handle_company_login(co)
            if not ok:
                return False, msg

            ok, msg = self._wait_for_gateway()
            if not ok:
                return False, f"Gateway not found after opening '{company_name}': {msg}"

            logger.info(f"[TallyLauncher] '{company_name}' ready ✓")
            return True, "ready"

        except Exception as e:
            logger.exception(f"[TallyLauncher] Unexpected error for '{company_name}'")
            return False, str(e)

    def _is_tally_running(self) -> bool:
        if HAS_PSUTIL:
            return any(
                'tally' in (p.info.get('name') or '').lower()
                for p in psutil.process_iter(['name'])
                if p.is_running()
            )
        return False

    def _kill_tally(self) -> Tuple[bool, str]:
        try:
            logger.info("[TallyLauncher] Killing Tally.exe...")
            result = subprocess.run(
                ["taskkill", "/F", "/IM", "tally.exe", "/T"],
                capture_output=True, text=True,
            )
            logger.info("[TallyLauncher] Tally killed ✓")
            return True, "killed"
        except Exception as e:
            return False, str(e)

    def _wait_for_tally_exit(self, timeout: int = 10) -> None:
        if not HAS_PSUTIL:
            time.sleep(3)
            return
        deadline = time.time() + timeout
        while time.time() < deadline:
            still_running = any(
                'tally' in (p.info.get('name') or '').lower()
                for p in psutil.process_iter(['name'])
                if p.is_running()
            )
            if not still_running:
                logger.info("[TallyLauncher] Tally process fully exited ✓")
                return
            time.sleep(0.5)
        logger.warning("[TallyLauncher] Tally still in process list — continuing anyway")

    def _launch_tally(self, exe_path: str) -> Tuple[bool, str]:

        if not exe_path:
            return False, "Tally.exe path not configured. Go to Settings → Automation."
        if not os.path.exists(exe_path):
            return False, f"Tally.exe not found at: {exe_path}"
        try:
            logger.info(f"[TallyLauncher] Launching: {exe_path}")
            subprocess.Popen(
                [exe_path],
                creationflags=(
                    subprocess.CREATE_NEW_PROCESS_GROUP
                    if hasattr(subprocess, 'CREATE_NEW_PROCESS_GROUP') else 0
                ),
            )

            logger.info("[TallyLauncher] Waiting for Tally window...")
            deadline = time.time() + 30
            while time.time() < deadline:
                if self._get_tally_window() is not None:
                    logger.info("[TallyLauncher] Tally window appeared ✓")
                    break
                time.sleep(0.5)

            logger.info("[TallyLauncher] Stabilizing (4s)...")
            time.sleep(4)

            return True, "launched"
        except Exception as e:
            return False, f"Failed to launch Tally: {e}"

    def _handle_tally_login(self) -> Tuple[bool, str]:

        tally_username = getattr(self._state, 'tally_username', '') or ''
        tally_password = getattr(self._state, 'tally_password', '') or ''

        if not tally_username:
            logger.info("[TallyLauncher] No Tally-level credentials configured — skipping login check")
            return True, "no_tally_login"

        logger.info("[TallyLauncher] Checking for Tally login screen (8s)...")
        found, _ = self._wait_for_image(self._img("username"), timeout=8)

        if not found:
            logger.info("[TallyLauncher] No Tally login screen — proceeding to Select Company")
            return True, "no_tally_login"

        logger.info("[TallyLauncher] Tally login screen found — entering credentials")
        self._bring_to_front()
        time.sleep(0.5)

        delay = self._click_delay()

        # Type username char by char — field is already focused after login screen appears
        self._type_interval(tally_username, interval=0.1)
        time.sleep(0.2)
        pyautogui.press('enter')
        time.sleep(delay)

        # Type password char by char
        self._type_interval(tally_password, interval=0.1)
        time.sleep(0.2)
        pyautogui.press('enter')
        time.sleep(delay * 3)

        logger.info("[TallyLauncher] Tally login submitted ✓")
        return True, "tally_logged_in"

    def _wait_for_select_company_screen(self) -> Tuple[bool, str]:

        logger.info("[TallyLauncher] Waiting for Select Company screen...")
        ok, msg = self._wait_for_image(self._img("search_box"), timeout=self._timeout())
        if ok:
            logger.info("[TallyLauncher] Select Company screen ready ✓")
            time.sleep(1)
        return ok, msg

    def _set_path_if_needed(self, co) -> Tuple[bool, str]:

        company_type = getattr(co, 'company_type', 'local') or 'local'

        if company_type == 'local':
            return True, "local_no_path_needed"

        elif company_type == 'remote':
            drive = (co.drive_letter or '').rstrip('\\').rstrip('/')
            path = co.data_path or ''
            if drive and path:
                full_path = f"{drive}\\{path.lstrip('/').lstrip('\\')}"
            elif drive:
                full_path = drive + "\\"
            else:
                full_path = path
            if full_path:
                ok, msg = self._set_data_path(full_path)
                if not ok:
                    logger.warning(f"[TallyLauncher] Remote path set failed ({msg}) — continuing")
            return True, "remote_path_set"

        elif company_type == 'tds':
            return self._set_tds_path(co)

        return True, "unknown_type_skipped"

    def _set_data_path(self, path: str) -> Tuple[bool, str]:
        delay = self._click_delay()
        ok, msg = self._click_image(self._img("change_path"))
        if not ok:
            return False, f"Change path button not found: {msg}"
        time.sleep(delay)
        self._type_interval(path, interval=0.05)
        time.sleep(0.2)
        pyautogui.press('enter')
        time.sleep(delay * 2)
        return True, "path_set"

    def _set_tds_path(self, co) -> Tuple[bool, str]:
        delay = self._click_delay()
        ok, msg = self._click_image(self._img("remote_tab"))
        if not ok:
            return False, f"Remote tab not found: {msg}"
        time.sleep(delay)
        if co.tds_path:
            ok, msg = self._click_image(self._img("tds_field"))
            if not ok:
                logger.warning(f"[TallyLauncher] TDS field not found: {msg}")
            else:
                self._type_interval(co.tds_path, interval=0.05)
                pyautogui.press('enter')
                time.sleep(delay * 2)
        if co.data_path:
            self._set_data_path(co.data_path)
        return True, "tds_set"

    @staticmethod
    def _clean_company_name(name) -> str:
        """
        Convert to str and strip whitespace.
        Tally search box matches the name exactly as displayed — type it as-is.
        """
        if name is None:
            return ''
        return str(name).strip()

    def _select_company(self, co) -> Tuple[bool, str]:

        self._bring_to_front()
        time.sleep(0.5)

        delay = self._click_delay()
        search_name = self._clean_company_name(co.name)
        logger.info(f"[TallyLauncher] Searching company: '{search_name}'")

        # Click the yellow search box to give it keyboard focus
        ok, _ = self._click_image(self._img("search_box"))
        if not ok:
            logger.warning("[TallyLauncher] Search box image not found — typing anyway")
            self._bring_to_front()
        time.sleep(0.5)

        # Type company name character by character at 0.1s interval
        # so Tally's live search filter can keep up with each keystroke
        self._type_interval(search_name, interval=0.1)
        time.sleep(1.0)   # wait for Tally list to fully filter

        logger.info("[TallyLauncher] Pressing Enter to open company")
        pyautogui.press('enter')
        time.sleep(delay * 3)

        return True, "company_selected"

    def _handle_company_login(self, co) -> Tuple[bool, str]:

        delay = self._click_delay()

        # Wait for the username image — if login dialog appears after Enter
        logger.info("[TallyLauncher] Waiting for company login dialog (10s)...")
        found, _ = self._wait_for_image(self._img("username"), timeout=10)

        if not found:
            logger.info("[TallyLauncher] No company login dialog — company opened directly ✓")
            return True, "no_company_login"

        username = getattr(co, 'tally_username', '') or ''
        password = getattr(co, 'tally_password', '') or ''

        logger.info(
            f"[TallyLauncher] Login dialog detected for '{co.name}' — "
            f"typing username: '{username}'"
        )

        # Type username char by char at 0.1s — field is already focused, no ctrl+a
        self._type_interval(username, interval=0.1)
        time.sleep(0.3)
        pyautogui.press('enter')
        time.sleep(delay * 1.5)

        # Type password char by char at 0.1s — no ctrl+a
        self._type_interval(password, interval=0.1)
        time.sleep(0.3)
        pyautogui.press('enter')
        time.sleep(delay * 2)

        # Confirm login dialog is dismissed
        still_showing, _ = self._wait_for_image(self._img("username"), timeout=4)
        if still_showing:
            return False, (
                f"Login failed for '{co.name}' — dialog still visible. "
                "Check username/password in Configure Company."
            )

        logger.info(f"[TallyLauncher] Company login successful ✓")
        return True, "company_logged_in"

    def _wait_for_gateway(self) -> Tuple[bool, str]:

        timeout = self._timeout()
        logger.info(f"[TallyLauncher] Waiting for Gateway (timeout={timeout}s)...")

        ok, msg = self._wait_for_image(self._img("gateway"), timeout=timeout)
        if ok:
            logger.info("[TallyLauncher] Gateway confirmed via image ✓")
            return True, "gateway_image_found"

        logger.warning("[TallyLauncher] Gateway image not found — trying XML fallback...")
        for attempt in range(5):
            try:
                from services.tally_connector import TallyConnector
                tc = TallyConnector(
                    host=self._state.tally.host,
                    port=self._state.tally.port,
                )
                if tc.status == "Connected":
                    logger.info("[TallyLauncher] Gateway confirmed via XML ✓")
                    return True, "gateway_xml_confirmed"
            except Exception as e:
                logger.debug(f"[TallyLauncher] XML attempt {attempt+1}: {e}")
            time.sleep(3)

        return False, "Gateway not confirmed — image not found and XML not responding"

    def _get_tally_window(self):
        if HAS_PYGETWINDOW:
            wins = gw.getWindowsWithTitle("Tally") or gw.getWindowsWithTitle("tally")
            return wins[0] if wins else None
        return None

    def _bring_to_front(self) -> Tuple[bool, str]:
        if not HAS_PYGETWINDOW:
            time.sleep(0.3)
            return True, "no_pygetwindow"
        win = self._get_tally_window()
        if not win:
            return False, "Tally window not found"
        try:
            if win.isMinimized:
                win.restore()
                time.sleep(0.5)
            win.activate()
            time.sleep(0.8)
            time.sleep(0.5)
            return True, "focused"
        except Exception as e:
            return False, str(e)

    def _get_all_open_companies(self, retries: int = 3) -> list:
        from services.tally_connector import TallyConnector
        host = self._state.tally.host
        port = self._state.tally.port
        for attempt in range(retries):
            try:
                tc = TallyConnector(host=host, port=port)
                if tc.status != "Connected":
                    time.sleep(2)
                    continue
                companies = tc.fetch_all_companies()
                return [c.get('name', '').strip() for c in companies if c.get('name', '').strip()]
            except Exception:
                if attempt < retries - 1:
                    time.sleep(2)
        return []

    def _locate_on_screen(self, img_path: str):
        try:
            if HAS_OPENCV:
                return pyautogui.locateOnScreen(
                    img_path, confidence=self._confidence(), grayscale=True
                )
            else:
                return pyautogui.locateOnScreen(img_path)
        except pyautogui.ImageNotFoundException:
            return None
        except Exception as e:
            err = str(e).lower()
            if "confidence" in err or "opencv" in err:
                try:
                    return pyautogui.locateOnScreen(img_path)
                except pyautogui.ImageNotFoundException:
                    return None
            return None

    def _wait_for_image(self, image_filename: str, timeout: int = None) -> Tuple[bool, str]:
        if timeout is None:
            timeout = self._timeout()
        img_path = os.path.join(_ASSETS_DIR, image_filename)
        if not os.path.exists(img_path):
            return False, f"Image file missing: {image_filename}"
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self._locate_on_screen(img_path):
                return True, "found"
            time.sleep(1)
        return False, f"Timeout {timeout}s — '{image_filename}' not found"

    def _click_image(self, image_filename: str) -> Tuple[bool, str]:
        img_path = os.path.join(_ASSETS_DIR, image_filename)
        if not os.path.exists(img_path):
            return False, f"Image file missing: {image_filename}"
        for attempt in range(self._retry_count()):
            loc = self._locate_on_screen(img_path)
            if loc:
                cx, cy = pyautogui.center(loc)
                pyautogui.click(cx, cy)
                return True, "clicked"
            time.sleep(1)
        return False, f"'{image_filename}' not found after {self._retry_count()} attempts"

    def _type_interval(self, text, interval: float = 0.1) -> None:
        """
        Convert input to str, then type character by character using
        pyautogui.press() with a fixed delay between each keystroke.
        No clipboard, no ctrl shortcuts — pure keystroke simulation.
        """
        if text is None:
            return
        text = str(text)
        if not text:
            return
        for ch in text:
            pyautogui.press(ch)
            time.sleep(interval)

    def _paste_text(self, text) -> None:
        """
        Convert input to str, then type using pyautogui.typewrite().
        No clipboard, no ctrl shortcuts.
        """
        if text is None:
            return
        text = str(text)
        if not text:
            return
        safe = ''.join(c if c.isascii() and c.isprintable() else '' for c in text)
        pyautogui.typewrite(safe, interval=0.05)

    def _type_text(self, text, char_interval: float = 0.0) -> None:
        """Convert to str and type. char_interval kept for compatibility."""
        self._paste_text(text)

    def _confidence(self) -> float:
        return float(getattr(self._state.automation, 'confidence', 0.80))

    def _click_delay(self) -> float:
        return float(getattr(self._state.automation, 'click_delay_ms', 500)) / 1000.0

    def _timeout(self) -> int:
        return int(getattr(self._state.automation, 'wait_timeout_sec', 30))

    def _retry_count(self) -> int:
        return int(getattr(self._state.automation, 'retry_attempts', 3))

    def _img(self, key: str) -> str:
        images = getattr(self._state, 'tally_images', {})
        defaults = {
            "gateway": "tally_gateway_screen.png",
            "search_box": "tally_company_search_box.png",
            "username": "tally_username_field.png",
            "password": "tally_password_field.png",
            "select_title": "tally_select_company_title.png",
            "change_path": "tally_change_path_btn.png",
            "remote_tab": "tally_remote_tab.png",
            "tds_field": "tally_tds_field.png",
        }
        return images.get(key, defaults.get(key, f"tally_{key}.png"))