from PyQt4.QtCore import QThread
from BWFlowTable import *

class ExcelFileThread(QThread):
    """
    This class is a class designed purely to hand off daisk and cpu intensive processing to put it in a thread
    It is passed the flow_table object which contains all the details of methods to populate excel statistics workbook
    The thread is initiated by instantiatiing the class and all relevant data for further processing passed into
    it (the __init__())
    The work is begun by calling the object start() method (as is usual when subclassing from QThread)
    This (in the background kicks off the run() method, which is overridden with code to read SAP table and load it to
    a sqlite database.
    messaging back to the gui is handled by signals in the self_flow_table object. This is typically in the form of
    updates about the stage of processing.
    """
    def __init__(self, flow_table, work_book_name):
        """
        SAP_Object has a connection to SAP (aka login) done before entry to this class sinceit requires GUI
        stuff which need to be in the main thread.
        :param flow_table, an instance of the BWFlowTable class
        :param work_book_name, excel work book name for output of statistics
        """
        super(ExcelFileThread, self).__init__()
        self._work_book_name = work_book_name
        self._flow_table = flow_table

    def run(self):
        self._flow_table.create_BW_stats(self._work_book_name)
        self.emit(SIGNAL('finished_excel_update'))