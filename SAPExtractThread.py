from PyQt4.QtCore import QThread
from BWFlowTable import *

class SAPExtractThread(QThread):
    """
    This class is a class designed purely to hand of database and cpu intensive processing to put it in a thread
    It is passed the flow_table object which contains all the details of SAP table reads to populate basic
    database tables, and the sql joins of those to create a flo table representing BW data flows.
    The thread is initiated by instantiatiing the class and all relevant data for further processing passed into
    it (the __init__())
    The work is begun by calling the object start() method (as is usual when subclassing from QThread)
    This (in the background kicks off the run() method, which is overridden with code to read SAP table and load it to
    a sqlite database.
    messaging back to the gui is handled by signals in the self_flow_table object. This is typically in the form of
    updates about the stage of processing.
    """
    def __init__(self, SAP_conn, flow_table, progress):
        """
        SAP_Object has a connection to SAP (aka login) done before entry to this class sinceit requires GUI
        stuff which need to be in the main thread.
        :param SAP_conn , connection object to SAP database
        :param flow_table, an instance of the BWFlowTable class
        :param progress, an integer denoting the distance along the progress bar in the GUI thread.
        """
        super(SAPExtractThread, self).__init__()
        self._SAP_conn = SAP_conn
        self._flow_table = flow_table
        self._progress = progress

    def run(self):
        self._flow_table.get_SAP_table_via_RFC(self._SAP_conn, self._progress)
        #Update flow table
        self._flow_table.create_flow_table()
        self._flow_table.update_text_table()
        self._flow_table.update_flow_from_RSUPDINFO()
        self._flow_table.update_flow_from_RSTRAN()
        self._flow_table.update_flow_from_RSIOSMAP()
        self._flow_table.update_flow_from_RSBSPOKE()
        self._flow_table.update_flow_from_RSBOHDEST()
        self._flow_table.update_flow_from_RSLDPSEL()
        self._flow_table.update_flow_from_RSDCUBEMULTI()
        self._flow_table.update_flow_from_RSRREPDIR()
        self._flow_table.update_flow_from_RSQTOBJ()
        self.emit(SIGNAL('finished_db_update'))
