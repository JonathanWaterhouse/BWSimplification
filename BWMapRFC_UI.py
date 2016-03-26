from sqlite3 import OperationalError
import PyQt4.QtGui
from BWMapping import Ui_BWMapping
from SAPExtractThread import *
from ExcelFileThread import *
from UserActivityThread import *
from SVGDisplay import *
import os
import pickle

__author__ = 'jonathan.waterhouse@gmail.com'

class BWMappingUI(Ui_BWMapping):
    def __init__(self, MainWindow):
        self.setupUi(MainWindow)
        self._otherGuiSetup()
        # Setup some internally required file locations
        dataDir = self.getDataDir() + os.sep
        self._iniFile = dataDir + "BWMapping.ini"
        dt = datetime.datetime.today()
        date_time = dt.strftime("%Y%m%d") + dt.strftime("%H%M%S")
        self._excel_stats_file = 'BWStatistics' + date_time + '.xlsx'
        self._ftp_server = 'directoryko.kodak.com' # ftp server for EKDIR statistics
        self._ftp_dir = 'pub' # ftp directory for EKDIR statistics
        self._ftp_file_cols = 'gd2000-header.txt' # ftp file containing EKDIR statistics col header names
        self._ftp_file_body = 'gd2000.extract' # ftp file containing EKDIR statistics data rows
        self._files = {}
        self._logged_in = False
        # Read the ini file containing the graphviz executable location
        try:
            f = open(self._iniFile, 'rb')
        except (IOError):
            self._files["DOT"] = ''
            f = open(self._iniFile, 'wb')
            pickle.dump(self._files, f)
            return
        else:
            try:
                self._files = pickle.load(f)
            except (pickle.UnpicklingError, EOFError):
                msg = PyQt4.QtGui.QMessageBox()
                msg.setText("Error")
                msg.setIcon(PyQt4.QtGui.QMessageBox.Critical)
                msg.setInformativeText(self._inifile + " is corrupt. Close application, delete the file, "
                                                       "then reopen the application")
                msg.exec_()
                raise IOError
            else:
                self._dot_file_loc = self._files["DOT"]

    def getDataDir(self):
        """
        This application may have a windows executable built from it using cx_Freeze in
        which case the local directly that the script runs from assumed by python
        will be incorrect. Here we derive the data directory. This allows the ini file
        Maestro.ini to be found and intermediate files for Graphviz
        """
        if getattr(sys, 'frozen', False):
            # The application is frozen
            datadir = os.path.dirname(sys.executable)
        else:
            # The application is not frozen
            # Change this bit to match where you store your data files:
            datadir = os.getcwd()

        return datadir

    def locate_dot(self):
        """Allow selection of the dot executable location on the windows workstation. This
        must have been installed first. Displays  file dialog to allow selection of file.
        The full paths are serialised in a pickle file so that they are available for
        future runs of the program, until next changed.
        File specification is performed via the File menu on the gui.
        """
        w = PyQt4.QtGui.QWidget()
        dotLoc = str(PyQt4.QtGui.QFileDialog.getOpenFileName(w, "Select dot executable file"))
        if dotLoc != "": self._files["DOT"] = dotLoc  # Allow cancellation and retain current value
        f = open(self._iniFile, 'wb')
        pickle.dump(self._files, f)
        return dotLoc

    def _otherGuiSetup(self):
        """ Do other setup things required to get the static GUI components set up, and
        components acting correctly upon user interaction. There should NOT be any
        code associated with date reading or population in here.
        """
        path = ''
        database = path + 'BWStructure.db'
        self._flow_table = BWFlowTable(database)
        self._graph_file = "BWGraph.svg"
        self._mini_graph_file = "BWMiniGraph.svg"

        # Actions
        self.generate_pushButton.clicked.connect(self.generateMap)
        self.fileRegenerate_Database.triggered.connect(self.generate_flow_Db)
        self.fileGenerate_User_Activity.triggered.connect(self.generate_user_activity)
        self.fileGenerate_Excel_Stats.triggered.connect(self.generate_excel_stats)
        self.actionLocate_dot.triggered.connect(self.locate_dot)
        self.exitButton.clicked.connect(self.exit)
        self.map_startpoint_combo.activated.connect(self.locate_node_by_index)
        PyQt4.QtCore.QObject.connect(self._flow_table, SIGNAL("show_msg"), self.show_msg)
        PyQt4.QtCore.QObject.connect(self._flow_table, SIGNAL("update_progress_bar"), self.update_progress_bar)

        # Populate combo box
        self.map_connectivity_combo.addItems(['Forward', 'Backward', 'Forward & Backward', 'All Connections'])
        try:
            for node in self._flow_table.get_nodes():
                self.map_startpoint_combo.addItem(node)
        except (OperationalError):
            msg = PyQt4.QtGui.QMessageBox()
            msg.setText("ERROR")
            msg.setIcon(PyQt4.QtGui.QMessageBox.Critical)
            msg.setInformativeText("There is no database configured. Please use command - File\Regenerate Db")
            msg.exec_()
            self.progressBar.setVisible(False)
            return
        self.map_startpoint_combo.setCurrentIndex(0)

        self.progressBar.setVisible(False)

    def locate_node_by_index(self, idx):
        if self.map_startpoint_combo.currentIndex() != -1:
            node = str(self.map_startpoint_combo.currentText())
            if self._flow_table.get_node(node):
                self.statusbar.showMessage("", 0)
                return
            else:
                self.statusbar.showMessage(node + " does not exist in BW Map.", 0)

    def generateMap(self):
        svg_file = ''
        extras = []
        if self.BI7_checkBox.isChecked(): extras.append('BI7 Converted')
        if self.flowVol_checkBox.isChecked(): extras.append('Flow Volumes last Run')
        if self.storedRec_checkBox.isChecked(): extras.append('Stored Record Count')
        if self.queries_checkBox.isChecked():
            show_queries = True
        else:
            show_queries = False

        try:
            graphviz_iterable = None
            if self.fullmap_radio.isChecked():  # generate full map
                svg_file = self._graph_file
                self.statusbar.showMessage("Creating graph in " + svg_file, 10000)
                graphviz_iterable = self._flow_table.create_full_graph()
                self.statusbar.showMessage("Graph created in " + svg_file, 10000)

            else:  # Generate partial map

                svg_file = self._mini_graph_file
                start_node = str(self.map_startpoint_combo.currentText())
                direction = str(self.map_connectivity_combo.currentText())

                # Check start node exists
                if not self._flow_table.get_node(start_node):
                    self.statusbar.showMessage(start_node + " does not exist in BW Map.", 0)
                    return
                else:
                    self.statusbar.showMessage("", 0)

                self.statusbar.showMessage("Creating graph in " + svg_file, 10000)
                # Get correct graph connection mode
                if direction == 'Forward':
                    forward = True
                    graphviz_iterable = self._flow_table.create_mini_graph_2(start_node, forward, show_queries)
                elif direction == 'Backward':
                    forward = False
                    graphviz_iterable = self._flow_table.create_mini_graph_2(start_node, forward, show_queries)
                elif direction == 'Forward & Backward':
                    forward = True
                    graphviz_iterable_1 = self._flow_table.create_mini_graph_2(start_node, forward, show_queries)
                    forward = False
                    graphviz_iterable_2 = self._flow_table.create_mini_graph_2(start_node, forward, show_queries)
                    graphviz_iterable = self._flow_table.add_graphviz_iterables(graphviz_iterable_1,
                                                                                graphviz_iterable_2)
                elif direction == 'All Connections':
                    graphviz_iterable = self._flow_table.create_mini_graph_connections(start_node, show_queries)
                else:
                    print ('Invalid mapping direction supplied')
        except (OperationalError):
            msg = PyQt4.QtGui.QMessageBox()
            msg.setText("ERROR")
            msg.setIcon(PyQt4.QtGui.QMessageBox.Critical)
            msg.setInformativeText("There is no database configured. Please use command - File\Regenerate Db")
            msg.exec_()
            self.statusbar.showMessage("There is no database configured. Please use command - File\Regenerate Db")
            return
        # Add decorations
        if 'BI7 Converted' in extras:
            graphviz_iterable = self._flow_table.decorate_graph_BI7Flow(graphviz_iterable)
            self.statusbar.showMessage("Generating BI7 conversion information", 10000)
        if 'Flow Volumes last Run' in extras:
            graphviz_iterable = self._flow_table.decorate_graph_flow_volumes(graphviz_iterable)
            self.statusbar.showMessage("Generating flow volumes information", 10000)
        if 'Stored Record Count' in extras:
            graphviz_iterable = self._flow_table.decorate_graph_sizes(graphviz_iterable)
            self.statusbar.showMessage("Generating data store size information", 10000)

        try:
            dot_exec_loc = self._files["DOT"]
            if dot_exec_loc == "": raise (KeyError)
        except (KeyError):
            self.statusbar.showMessage("Graphviz 'dot' executable location must be selected.")
            msg = PyQt4.QtGui.QMessageBox()
            msg.setText("Warning")
            msg.setInformativeText("Graphviz 'dot' executable location must be selected. Please use file menu")
            msg.setIcon(PyQt4.QtGui.QMessageBox.Warning)
            msg.exec_()
            return

        self._flow_table.create_svg_file(graphviz_iterable, svg_file, dot_exec_loc)
        self.statusbar.showMessage("Graph created in " + svg_file, 10000)

        dataDir = os.getcwd() + os.sep
        SVGDisplay(self, dataDir + svg_file, self._flow_table)
        return

    def generate_flow_Db(self):
        """
        This method reads SAP BW for all necessary metadata tabes and adds them to a sqlite database. From them
        it constructs a flow table which represents all data flow paths through BW. It has everal stages
        a) Login to SAP
        b) Create a thread to do the database reads and writes
        c) The thread once executing starts BWFlow table methods which via Qt signals and slots sends messages to this
        modules' methods to update message bar and progress bar.
        d) because its in a thread, we an cancel the whole operation from the GUI at any time (by closing the GUI)
        """
        # Show progress since this is a long running operation
        i = 0
        self._max_status_bar_value = len(self._flow_table.tab_spec) + 1
        self.progressBar.setMinimum(i)
        self.progressBar.setMaximum(self._max_status_bar_value)
        self.progressBar.setVisible(True)
        # Update tables from SAP
        self.statusbar.showMessage("Regenerating.", 0)
        #TODO Not sure why this try, except clause is here. It may not be required, being a remnant of another approach
        try:
            if not self._logged_in:
                self.login_to_SAP()  # Essentially creates a connection object, via method set_SAP+connection called from login UI
            # Handle cancellation of login
            if not self._logged_in:
                self.progressBar.setValue(self._max_status_bar_value)
                self.progressBar.setVisible(False)
                self.statusbar.showMessage("Db tables regeneration cancelled.", 10000)
                return
            else:
                # Handle successful login
                # a) self._SAP_conn comes from running self.login_to SAP() above
                # b) self._flow_table is set up in method otherGUISetup() of this class
                self._flow_table_thread = SAPExtractThread(self._SAP_conn, self._flow_table, i)
                PyQt4.QtCore.QObject.connect(self._flow_table_thread, SIGNAL("finished_db_update"),
                                             self.finished_processing)
                self._flow_table_thread.start()
                return

        except IOError:
            msg = PyQt4.QtGui.QMessageBox()
            msg.setText("ERROR")
            msg.setIcon(PyQt4.QtGui.QMessageBox.Critical)
            msg.setInformativeText("File " + file + " cannot be found.")
            msg.exec_()
            self.progressBar.setVisible(False)
            self.statusbar.showMessage("Error populating database.", 0)
            return  # on error

    def generate_user_activity(self):
        if not self._logged_in:
            self.login_to_SAP()  # Essentially creates a connection object, via method set_SAP+connection called from login UI
        # Handle cancellation of login
        if not self._logged_in:
            self.statusbar.showMessage("User activity regeneration cancelled.", 10000)
            return
        else:
            # Handle successful login
            # a) self._SAP_conn comes from running self.login_to SAP() above
            # b) self._flow_table is set up in method otherGUISetup() of this class
            self._user_activity_thread = UserActivityThread(self._SAP_conn, self._flow_table, self._ftp_server,
                                                            self._ftp_dir, self._ftp_file_cols, self._ftp_file_body)
            self._user_activity_thread.start()
            #self._flow_table.get_user_activity_LISTCUBE_via_RFC()
            #self._flow_table.get_EKDIR_data(self._ftp_server, self._ftp_dir, self._ftp_file_cols, self._ftp_file_body)
            return

    def generate_excel_stats(self):
        """
        This method reads the local sqlite flow database and creates various statistcis. each separate statistic
        in a separate worksheet within an excel workbook
        """
        i = 0
        self._max_status_bar_value = 7
        self.progressBar.setMinimum(i)
        self.progressBar.setMaximum(self._max_status_bar_value)
        self.progressBar.setVisible(True)
        self.statusbar.showMessage("Generating Excel file.", 0)
        self._excel_file_write = ExcelFileThread(self._flow_table, self._excel_stats_file)
        PyQt4.QtCore.QObject.connect(self._excel_file_write, SIGNAL("finished_excel_update"),
                                     self.finished_excel_processing)
        self._excel_file_write.start()
        return
        #self._flow_table.create_BW_stats(self._excel_stats_file)
        #self.statusbar.showMessage("Excel file " + self._excel_stats_file + " created.", 10000)

    def finished_excel_processing(self):
        """
        This method is connected to event "finished_excel_update" emitted by the ExcelFileThread object doing the
        excel statistics creation. It shows a completed message.
        :return: nothing
        """
        self.progressBar.setValue(self._max_status_bar_value)
        self.progressBar.setVisible(False)
        self.statusbar.showMessage("Excel file " + self._excel_stats_file + " created.", 10000)
        return

    def show_msg(self, msg):
        """
        This method is connected to event "show_messsage" emitted by the BWFlowTable doing the
        database read and load. It sets a screen message.
        :param result: Contains a message
        :return: nothing
        """
        self.statusbar.showMessage(msg, 0)
        return

    def update_progress_bar(self, tableNumInt):
        """
        This method is connected to event "update_progress_bar" emitted by the BWFlowTable doing the
        database read and load. It advances the progress bar progress indicator.
        :param tableNumInt: Integer indicating distance along the progress bar
        :return: nothing
        """
        self.progressBar.setValue(tableNumInt)
        return

    def finished_processing(self):
        """
        This method is connected to event "finished_db_update" emitted by the SAPExtractThread object doing the
        database read and load. Does some screen tidy up
        :return: nothing
        """
        self.progressBar.setValue(self._max_status_bar_value)
        self.progressBar.setVisible(False)
        self.statusbar.showMessage("Db tables regenerated.", 10000)
        # Populate combo box of possible starting nodes for mapping
        for node in self._flow_table.get_nodes(): self.map_startpoint_combo.addItem(node)
        return

    def login_to_SAP(self):
        """
        Login to SAP. To do this a simple UI is presented to allow user input of credentials and target system. This
        avoids any hard coding of credentials and flexibility as to target system eg dev, QA, Prod.
        :return: None. Sets a class connection object used by other methods by calling the parent class (ie this one)
        set_SAP_connection method..
        """
        login_ui = SAPLogonUI(self)

    def set_SAP_connection(self, conn):
        """
        Called by SAPLogonUI to pass the SAP connection string to this class
        """
        self._SAP_conn = conn
        if self._SAP_conn == None:
            self.logged_in = False  # If login was cancelled
        else:
            self._logged_in = True

    def exit(self):
        sys.exit(0)


if __name__ == '__main__':
    import sys

    app = PyQt4.QtGui.QApplication(sys.argv)
    MainWindow = PyQt4.QtGui.QMainWindow()
    ui = BWMappingUI(MainWindow)
    MainWindow.show()
    sys.exit(app.exec_())
    # TODO Option to display inactive (why does ZOHCRM003 show which has inactive update rule).
    # TODO GUI does not update during heavy processing (use threads?)
    # TODO Some mechanism to identify a flow associated with a process area eg CMIS which has many disconnected flows
    # TODO     This could be as simple as populating datastore names in the dropdown list box (and allowing sorting by text)
    # TODO     to allow easy identification of relevant datastores. Alternatively some sort of wider map display that is
    # TODO     not everything
    # TODO Links via lookup display
    # TODO Python distribution (not cx_freeze)
    # TODO Automated way to get object sizes rather than manual run of DB02
    # TODO Status bar to do a better job of subdividing the lengthy tasks
    # TODO Object links due to ABAP lookup.
