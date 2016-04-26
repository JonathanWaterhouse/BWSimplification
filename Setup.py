import sys
from cx_Freeze import setup, Executable

# Dependencies are automatically detected, but it might need fine tuning.
#build_exe_options = {"packages": ["decimal"]}
packages = []
include_files = ['thread_16xLG.png']

# GUI applications require a different base on Windows (the default is for a
# console application).
base = None
if sys.platform == "win32":
    base = "Win32GUI"
    
setup(name='BWMap',
      version='3.02',
      description='BW Mapping Tool',
	  options = {"build_exe":  { 'packages' : packages, 'include_files': include_files}},
      executables = [Executable("BWMapRFC_UI.py", base=base)]
      )
