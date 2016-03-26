from PyQt4.QtCore import QThread
from BWFlowTable import *

class UserActivityThread(QThread):
    """
    This class is a class designed purely to hand off disk and cpu intensive processing to put it in a thread
    It is passed the flow_table object which contains all the details of methods to extract LISTCUBE from SAP BW
    of user activity and place it in a sqlite database.
    The thread is initiated by instantiatiing the class and all relevant data for further processing passed into
    it (the __init__())
    The work is begun by calling the object start() method (as is usual when subclassing from QThread)
    This (in the background kicks off the run() method, which is overridden with code to read SAP table and load it to
    a sqlite database.
    messaging back to the gui is handled by signals in the self_flow_table object. This is typically in the form of
    updates about the stage of processing.
    """
    def __init__(self, SAP_conn, flow_table, ftp_server, ftp_dir, ftp_file_cols, ftp_file_body):
        """
        SAP_Object has a connection to SAP (aka login) done before entry to this class sinceit requires GUI
        stuff which need to be in the main thread.
        :param SAP_conn, SAP Connection object
        :param flow_table, an instance of the BWFlowTable class
        :param ftp_server, ftp server for EKDIR statistics
        :param ftp_dir, ftp directory for EKDIR statistics
        :param ftp_file_cols, ftp file containing EKDIR statistics col header names
        :param ftp_file_body, ftp file containing EKDIR statistics data rows
        """
        super(UserActivityThread, self).__init__()
        self._SAP_conn = SAP_conn
        self._flow_table = flow_table
        self._ftp_server = ftp_server
        self._ftp_dir = ftp_dir
        self._ftp_file_cols = ftp_file_cols
        self._ftp_file_body = ftp_file_body

    def run(self):
        self._flow_table.get_user_activity_LISTCUBE_via_RFC(self._SAP_conn)
        self._flow_table.get_EKDIR_data(self._ftp_server, self._ftp_dir, self._ftp_file_cols, self._ftp_file_body)