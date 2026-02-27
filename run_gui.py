"""
run_gui.py
==========
Entry point — run this to launch the Tally Sync Manager desktop app.

    python run_gui.py
"""
import sys
import os

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from gui.app import TallySyncApp


def main():
    app = TallySyncApp()
    app.run()


if __name__ == "__main__":
    main()