from distutils.core import setup
import sys, os
from cx_Freeze import setup, Executable
sys.path.append(os.path.dirname(os.getcwd()))
# Dependencies are automatically detected, but it might need fine tuning.
build_exe_options = {'packages':["PyQT4.QtNetwork"], "include_files":["thread_16xLG.png"]}
# GUI applications require a different base on Windows (the default is for a
# console application).
base = None
if sys.platform == "win32":
    base = "Win32GUI"
    
setup(name='BWMapping',
      version='1.0',
      description='BW Map Viewer',
      options = {"build_exe": build_exe_options},
      executables = [Executable("BWMappingUI.py", base=base)]
      )
