__author__ = 'U104675'
from PyQt4 import QtCore
class BWThread(QtCore.QThread):
    def __init__(self, flow_table, status_bar, progress_bar, i):
        QtCore.QThread.__init__(self)
        self._flow_table = flow_table
        self._status_bar = status_bar
        self._progress_bar = progress_bar
        self._i = i

    def run(self):
        self._flow_table.get_SAP_table_via_RFC(self._status_bar,self._progress_bar, self._i)
        return
