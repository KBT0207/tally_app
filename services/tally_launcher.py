import os
import sys
import time
import subprocess
from typing import Tuple
from datetime import datetime

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


def _get_assets_dir() -> str:
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        return os.path.join(sys._MEIPASS, "assets")
    return os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "assets")

ASSETS_DIR = _get_assets_dir()

IMAGE_FILES = {
    "gateway":      "tally_gateway_screen.png",
    "search_box":   "tally_company_search_box.png",
    "username":     "tally_username_field.png",
    "data_server":  "tally_dataserver_image.png",
    "local_path":   "tally_local_path_image.png",
    "select_company":"tally_select_company_title.png",
    "change_period": "tally_change_period.png",
}


class TallyLauncher:

    def __init__(self, state):
        self.state = state

    def prepare(self, company_name: str) -> Tuple[bool, str]:
        if not HAS_PYAUTOGUI:
            return False, "PyAutoGUI not installed"

        company = self.state.companies.get(company_name)
        if not company:
            return False, f"Company '{company_name}' not found"

        tally_exe    = getattr(self.state, 'tally_exe_path', '') or ''
        company_type = getattr(company, 'company_type', 'local') or 'local'

        logger.info(f"[TallyLauncher] Starting: '{company_name}' (type={company_type})")

        try:
            if self.is_tally_running():
                self.kill_tally()
                self.wait_for_tally_to_close()

            ok, msg = self.open_tally(tally_exe)
            if not ok: return False, msg

            ok, msg = self.handle_tally_login()
            if not ok: return False, msg

            ok, msg = self.wait_for_company_list()
            if not ok: return False, msg

            if company_type == 'tds':
                ok, msg = self.navigate_to_tds_data_server()
                if not ok: return False, msg

            ok, msg = self.select_company(company)
            if not ok: return False, msg

            ok, msg = self.handle_company_login(company, company_type)
            if not ok: return False, msg

            ok, msg = self.wait_for_gateway()
            if not ok: return False, msg

            ok, msg = self.change_period(company)
            if not ok:
                logger.warning(f"[TallyLauncher] Change period failed: {msg}")

            logger.info(f"[TallyLauncher] '{company_name}' is ready ✓")
            return True, "ready"

        except Exception as e:
            logger.exception(f"[TallyLauncher] Unexpected error for '{company_name}'")
            return False, str(e)

    def change_period(self, company) -> Tuple[bool, str]:
        try:
            self.bring_tally_to_front()
            time.sleep(1)

            pyautogui.hotkey('alt', 'f2')
            
            if not self.wait_for_image("change_period"):
                return False, "Change Period dialog did not appear"

            raw_start = getattr(company, 'starting_from', None)
            if not raw_start:
                return False, "starting_from date missing"

            clean = str(raw_start).replace("-", "")[:8]
            formatted_start = f"{clean[6:8]}-{clean[4:6]}-{clean[0:4]}"
            formatted_today = datetime.now().strftime("%d-%m-%Y")

            self.type_text(formatted_start)
            pyautogui.press('enter')
            time.sleep(0.5)
            self.type_text(formatted_today)
            pyautogui.press('enter')

            self._wait_for_image_to_disappear("change_period")
            logger.info(f"[TallyLauncher] Period changed to: {formatted_start} to {formatted_today}")
            return True, "period_changed"
            
        except Exception as e:
            return False, str(e)

    def _wait_for_image_to_disappear(self, image_key: str, seconds: int = None) -> bool:
        if seconds is None:
            seconds = self.get_timeout()

        images   = getattr(self.state, 'tally_images', {}) or {}
        filename = images.get(image_key) or IMAGE_FILES.get(image_key) or f"tally_{image_key}.png"
        img_path = os.path.join(ASSETS_DIR, filename)
        
        start = time.time()
        while (time.time() - start) < seconds:
            if not self.find_image_on_screen(img_path):
                return True
            time.sleep(1)
        return False

    def close_tally(self) -> Tuple[bool, str]:
        if not self.is_tally_running():
            return True, "not_running"
        ok, msg = self.kill_tally()
        if ok:
            self.wait_for_tally_to_close()
        return ok, msg

    def is_tally_running(self) -> bool:
        if HAS_PSUTIL:
            for p in psutil.process_iter(['name']):
                if 'tally' in (p.info.get('name') or '').lower():
                    return True
            return False
        result = subprocess.run(["tasklist", "/FI", "IMAGENAME eq tally.exe", "/NH"], capture_output=True, text=True)
        return "tally.exe" in result.stdout.lower()

    def kill_tally(self) -> Tuple[bool, str]:
        subprocess.run(["taskkill", "/F", "/IM", "tally.exe", "/T"], capture_output=True, text=True)
        return True, "killed"

    def wait_for_tally_to_close(self):
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
                time.sleep(2)
                return
            time.sleep(1)

    def open_tally(self, exe_path: str) -> Tuple[bool, str]:
        if not exe_path or not os.path.exists(exe_path):
            return False, "Tally path error"
        
        subprocess.Popen([exe_path])

        found = False
        for _ in range(30):
            if self.get_tally_window() is not None:
                found = True
                break
            time.sleep(1)

        if not found:
            return False, "tally window not found"

        # if not self._select_company(interval=60):
        #     return False, "select_company not found"

        self.wait_for_image("select_company", seconds=self.get_timeout())
        
        return True, "launched"

    def handle_tally_login(self) -> Tuple[bool, str]:
        username = getattr(self.state, 'tally_username', '') or ''
        password = getattr(self.state, 'tally_password', '') or ''
        if not username: return True, "skipped"
        if self.wait_for_image("username"):
            self.bring_tally_to_front()
            self.type_text(username)
            pyautogui.press('enter')
            time.sleep(1)
            self.type_text(password)
            pyautogui.press('enter')
        return True, "done"

    def wait_for_company_list(self) -> Tuple[bool, str]:
        if self.wait_for_image("search_box"):
            time.sleep(1)
            return True, "ready"
        return False, "timeout"

    def navigate_to_tds_data_server(self) -> Tuple[bool, str]:
        self.bring_tally_to_front()
        if not self.double_click_image("data_server"): return False, "no_btn"
        pyautogui.moveTo(5, 5)
        if not self.wait_for_image("local_path"): return False, "no_path"
        if not self.wait_for_image("search_box"): return False, "no_reload"
        return True, "tds_ready"

    def select_company(self, company) -> Tuple[bool, str]:
        self.bring_tally_to_front()
        name = str(company.name or '').strip()
        self.click_image("search_box")
        self.type_text(name)
        time.sleep(1)
        pyautogui.press('enter')
        return True, "selected"

    def handle_company_login(self, company, company_type: str) -> Tuple[bool, str]:
        username = getattr(company, 'tally_username', '') or ''
        password = getattr(company, 'tally_password', '') or ''
        if company_type == 'tds':
            if not username: return True, "no_creds"
            time.sleep(3)
            self.bring_tally_to_front()
            self.type_text(username)
            pyautogui.press('enter')
            time.sleep(1)
            self.type_text(password)
            pyautogui.press('enter')
            return True, "logged_in"
        if not self.wait_for_image("username"): return True, "no_dialog"
        self.bring_tally_to_front()
        if not username:
            pyautogui.press('enter', presses=2)
            return True, "skipped"
        self.type_text(username)
        pyautogui.press('enter')
        time.sleep(1)
        self.type_text(password)
        pyautogui.press('enter')
        return True, "done"

    def wait_for_gateway(self) -> Tuple[bool, str]:
        if self.wait_for_image("gateway"):
            return True, "gateway_found"
        return False, "timeout"

    def get_tally_window(self):
        if not HAS_PYGETWINDOW: return None
        wins = gw.getWindowsWithTitle("Tally") or gw.getWindowsWithTitle("tally")
        return wins[0] if wins else None

    def bring_tally_to_front(self):
        win = self.get_tally_window()
        if win:
            try:
                if win.isMinimized: win.restore()
                win.activate()
                time.sleep(0.8)
            except: pass

    def find_image_on_screen(self, img_path: str):
        try:
            conf = self.get_confidence()
            return pyautogui.locateOnScreen(img_path, confidence=conf, grayscale=True) if HAS_OPENCV else pyautogui.locateOnScreen(img_path)
        except: return None

    def wait_for_image(self, image_key: str, seconds: int = None) -> bool:
        if seconds is None:
            seconds = self.get_timeout()

        images   = getattr(self.state, 'tally_images', {}) or {}
        filename = images.get(image_key) or IMAGE_FILES.get(image_key) or f"tally_{image_key}.png"
        img_path = os.path.join(ASSETS_DIR, filename)
        if not os.path.exists(img_path): return False

        end = time.time() + seconds
        while time.time() < end:
            if self.find_image_on_screen(img_path): return True
            time.sleep(1)
        return False

    def _select_company(self, interval: int = 60, timeout: int = None) -> bool:
        if timeout is None:
            timeout = self.get_timeout()

        start = time.time()
        while (time.time() - start) < timeout:
            if self.wait_for_image("select_company", seconds=2):
                return True
            self.bring_tally_to_front()
            pyautogui.press('enter')
            time.sleep(min(interval, 5))
        return False

    def click_image(self, image_key: str) -> bool:
        images   = getattr(self.state, 'tally_images', {}) or {}
        filename = images.get(image_key) or IMAGE_FILES.get(image_key) or f"tally_{image_key}.png"
        img_path = os.path.join(ASSETS_DIR, filename)
        loc = self.find_image_on_screen(img_path)
        if loc:
            pyautogui.click(pyautogui.center(loc))
            return True
        return False

    def double_click_image(self, image_key: str) -> bool:
        images   = getattr(self.state, 'tally_images', {}) or {}
        filename = images.get(image_key) or IMAGE_FILES.get(image_key) or f"tally_{image_key}.png"
        img_path = os.path.join(ASSETS_DIR, filename)
        loc = self.find_image_on_screen(img_path)
        if loc:
            pyautogui.doubleClick(pyautogui.center(loc))
            return True
        return False

    def type_text(self, text) -> None:
        if text: pyautogui.write(str(text), interval=0.1)

    def get_confidence(self) -> float:
        aut = getattr(self.state, 'automation', None)
        return float(getattr(aut, 'confidence', 0.80)) if aut else 0.80

    def get_timeout(self) -> int:
        aut = getattr(self.state, 'automation', None)
        timeout = getattr(aut, 'wait_timeout_sec', None) if aut else None
        return int(timeout) if timeout is not None else 300