import pyautogui as pg
import time
from pyutils import wait_image



def open_tally(path=r"C:\Program Files\TallyPrime\tally.exe"):
    pg.hotkey('win', 'r')
    time.sleep(0.5)
    pg.write(path,interval=0.1)
    time.sleep(0.5)
    pg.press('enter')
    wait_image()

    

if __name__ == "__main__":
    open_tally()