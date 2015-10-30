from distutils.core import setup
#import sys
#from cx_Freeze import setup, Executable

# Dependencies are automatically detected, but it might need fine tuning.
#build_exe_options = {"packages": ["decimal"]}

# GUI applications require a different base on Windows (the default is for a
# console application).
#base = None
#if sys.platform == "win32":
#    base = "Win32GUI"
    
#setup(name='BWMap',
#      version='2.2',
#      description='BW Mapping Tool',
#	  options = {"build_exe": build_exe_options},
#      executables = [Executable("BWMapRFC_UI.py", base=base)]
#      )
from setuptools import setup, find_packages
setup(
    name = "BWMap",
    version = "2.4",
    packages = find_packages(),
)