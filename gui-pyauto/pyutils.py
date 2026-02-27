import pyautogui as pg
import time

def wait_image(path=r"E:\tally_project\gui-pyauto\images\select_company.png"):
    loc = None
    while loc is None:
        try:
            loc = pg.locateOnScreen(path, confidence=0.9)
            if loc:
                print('Tally Opened Successfully')
                return loc
        except pg.ImageNotFoundException:
            pass



def comp_list():
    pass

        
    

def press():
    pass

