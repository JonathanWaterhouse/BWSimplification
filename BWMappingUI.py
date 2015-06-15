from sqlite3 import OperationalError
from PyQt5.QtWidgets import QApplication, QMainWindow, QMessageBox, QWidget, QFileDialog
from BWMapping import Ui_BWMapping
from BWFlowTable import BWFlowTable
from SVGDisplay import *
import os
import pickle

__author__ = 'U104675'
import BWMapping
class BWMappingUI(Ui_BWMapping):
    def __init__(self,MainWindow):
        self.setupUi(MainWindow)
        self._otherGuiSetup()
        #Setup some internally required file locations
        dataDir = self.getDataDir() + os.sep
        self._iniFile = dataDir + "BWMapping.ini"
        self._files= {}
        # location of text files to populate sqlite tables
        self._RSLDPSEL_fil = "RSLDPSEL.txt" # This file has one line split over two
        self._RSLDPSEL_unsplit_fil = "RSLDPSELUnsplit.txt" # After joining the two line halves this is the new filename
        self._table_file_map = dict(RSTRAN="RSTRAN.txt", RSISOSMAP="RSISOSMAP.txt",
                    RSLDPSEL=self._RSLDPSEL_unsplit_fil, RSLDPIO="RSLDPIO.txt",
                    RSLDPIOT="RSLDPIOT.txt", RSBSPOKE="RSBSPOKE.txt", RSBOHDEST="RSBOHDEST.txt",
                    RSIS="RSIS.txt", RSUPDINFO="RSUPDINFO.txt", RSDCUBEMULTI='RSDCUBEMULTI.txt',
                    RSRREPDIR='RSRREPDIR.txt', RSDCUBET='RSDCUBET.txt', RSZELTDIR='RSZELTDIR.txt',
                    RSMDATASTATE_EXT='RSMDATASTATE_EXT.txt', RSSTATMANPART='RSSTATMANPART.txt',
                    RSDODSOT='RSDODSOT.txt')
        # Read the ini file containing the graphviz executable location
        try:
            f = open(self._iniFile,'rb')
        except (FileNotFoundError):
            self._files["DOT"] = ''
            f = open(self._iniFile,'wb')
            pickle.dump(self._files,f)
            return
        else:
            try:
                self._files = pickle.load(f)
            except (pickle.UnpicklingError, EOFError):
                msg = QMessageBox()
                msg.setText("Error")
                msg.setIcon(QMessageBox.Critical)
                msg.setInformativeText(self._inifile + " is corrupt. Close application, delete the file, "
                                       "then reopen the application")
                msg.exec()
                raise(FileNotFoundError)
            else: self._dot_file_loc = self._files["DOT"]

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
        w = QWidget()
        dotLoc = QFileDialog.getOpenFileName(w,"Select dot executable file")[0]
        if dotLoc != "": self._files["DOT"] = dotLoc #Allow cancellation and retain current value
        f = open(self._iniFile,'wb')
        pickle.dump(self._files,f)
        return dotLoc
        return

    def _otherGuiSetup(self):
        """ Do other setup things required to get the static GUI components set up, and
        components acting correctly upon user interaction. There should NOT be any
        code associated with date reading or population in here.
        """
        #Actions
        self.generate_pushButton.clicked.connect(self.generateMap)
        self.fileRegenerate_Database.triggered.connect(self.generateDb)
        self.actionLocate_dot.triggered.connect(self.locate_dot)
        self.exitButton.clicked.connect(self.exit)
        self.map_startpoint_combo.activated.connect(self.locate_node_by_index)

        path = ''
        database = path + 'BWStructure.db'
        self._t = BWFlowTable(database)
        self._graph_file = "BWGraph.svg"
        self._mini_graph_file = "BWMiniGraph.svg"

        # Populate combo box
        self.map_connectivity_combo.addItems(['Forward','Backward', 'Forward & Backward','All Connections'])
        try:
            for node in self._t.get_nodes(): self.map_startpoint_combo.addItem(node)
        except (OperationalError):
            msg = QMessageBox()
            msg.setText("ERROR")
            msg.setIcon(QMessageBox.Critical)
            msg.setInformativeText("There is no database configured. Please use command - File\Regenerate Db")
            msg.exec()
            self.progressBar.setVisible(False)
            return
        self.map_startpoint_combo.setCurrentIndex(0)

        self.progressBar.setVisible(False)

    def locate_node_by_index(self,idx):
        if self.map_startpoint_combo.currentIndex() != -1:
            node = self.map_startpoint_combo.currentText()
            if self._t.get_node(node):
                self.statusbar.showMessage("", 0)
                return
            else: self.statusbar.showMessage(node + " does not exist in BW Map.", 0)

    def generateMap(self):
        svg_file = ''
        extras = []
        if self.BI7_checkBox.isChecked(): extras.append('BI7 Converted')
        if self.flowVol_checkBox.isChecked(): extras.append('Flow Volumes last Run')
        if self.storedRec_checkBox.isChecked(): extras.append('Stored Record Count')
        if self.queries_checkBox.isChecked(): show_queries = True
        else: show_queries = False

        try:
            graphviz_iterable = None
            if self.fullmap_radio.isChecked(): # generate full map
                svg_file = self._graph_file
                self.statusbar.showMessage("Creating graph in " + svg_file,10000)
                graphviz_iterable = self._t.create_full_graph()
                self.statusbar.showMessage("Graph created in " + svg_file,10000)

            else: # Generate partial map

                svg_file = self._mini_graph_file
                start_node = self.map_startpoint_combo.currentText()
                direction = self.map_connectivity_combo.currentText()

                #Check start node exists
                if not self._t.get_node(start_node):
                    self.statusbar.showMessage(start_node + " does not exist in BW Map.", 0)
                    return
                else: self.statusbar.showMessage("", 0)

                self.statusbar.showMessage("Creating graph in " + svg_file,10000)
                #Get correct graph connection mode
                if direction == 'Forward' :
                    forward = True
                    graphviz_iterable = self._t.create_mini_graph(start_node,forward, show_queries)
                elif direction == 'Backward' :
                    forward = False
                    graphviz_iterable = self._t.create_mini_graph(start_node,forward, show_queries)
                elif direction == 'Forward & Backward' :
                    graphviz_iterable = self._t.create_mini_graph_bwd_fwd(start_node, show_queries)
                elif direction == 'All Connections' :
                    graphviz_iterable = self._t.create_mini_graph_connections(start_node, show_queries)
                else: print ('Invalid mapping direction supplied')
        except (OperationalError):
            msg = QMessageBox()
            msg.setText("ERROR")
            msg.setIcon(QMessageBox.Critical)
            msg.setInformativeText("There is no database configured. Please use command - File\Regenerate Db")
            msg.exec()
            self.statusbar.showMessage("There is no database configured. Please use command - File\Regenerate Db")
            return
        #Add decorations
        if 'BI7 Converted' in extras:
            graphviz_iterable = self._t.decorate_graph_BI7Flow(graphviz_iterable)
            self.statusbar.showMessage("Generating BI7 conversion information",10000)
        if 'Flow Volumes last Run' in extras:
            graphviz_iterable = self._t.decorate_graph_flow_volumes(graphviz_iterable)
            self.statusbar.showMessage("Generating flow volumes information",10000)
        if 'Stored Record Count' in extras:
            graphviz_iterable = self._t.decorate_graph_sizes(graphviz_iterable)
            self.statusbar.showMessage("Generating data store size information",10000)

        try:
            dot_exec_loc = self._files["DOT"]
            if dot_exec_loc == "": raise (KeyError)
        except (KeyError):
            self.statusbar.showMessage("Graphviz 'dot' executable location must be selected.")
            msg = QMessageBox()
            msg.setText("Warning")
            msg.setInformativeText("Graphviz 'dot' executable location must be selected. Please use file menu")
            msg.setIcon(QMessageBox.Warning)
            msg.exec()
            return

        self._t.create_svg_file(graphviz_iterable,svg_file,dot_exec_loc)
        self.statusbar.showMessage("Graph created in " + svg_file,10000)

        dataDir = os.getcwd() + os.sep
        SVGDisplay(self, dataDir + svg_file, self._t)
        return

    def generateDb(self):
        unwanted_cols = []
        #Show progress since this is a long running operation
        i=0
        self.progressBar.setMinimum(i)
        self.progressBar.setMaximum(len(self._table_file_map)+ 1)
        self.progressBar.setVisible(True)
        #Check file names are initialised
        if len(self._table_file_map) == 0:
            msg = QMessageBox()
            msg.setText("ERROR")
            msg.setIcon(QMessageBox.Critical)
            msg.setInformativeText("Internal program error. No database table source text file names specified.")
            msg.exec()
            self.progressBar.setVisible(False)
            return
        #Unsplit RSLDPSEL file
        try:
            sep = "|" # fieldseparator in the file
            self._t._unsplit(self._RSLDPSEL_fil, self._RSLDPSEL_unsplit_fil, sep)
        except(FileNotFoundError):
            msg = QMessageBox()
            msg.setText("ERROR")
            msg.setIcon(QMessageBox.Critical)
            msg.setInformativeText("Database table source text file RSLDPSEL is missing. Please supply before continuing.")
            msg.exec()
            self.progressBar.setVisible(False)
            return
        #Update tables from SAP
        for table, file in self._table_file_map.items():
            self.statusbar.showMessage("Regenerating " + table,0)
            try:
                self._t.update_table({table : file}, unwanted_cols)
            except (FileNotFoundError):
                msg = QMessageBox()
                msg.setText("ERROR")
                msg.setIcon(QMessageBox.Critical)
                msg.setInformativeText("File " + file + " cannot be found.")
                msg.exec()
                self.progressBar.setVisible(False)
                self.statusbar.showMessage("Error populating database.",0)
                return
            i += 1
            self.progressBar.setValue(i)
        #Update flow table
        self.statusbar.showMessage("Regenerating flow table",0)
        self._t.create_flow_table()
        self._t.update_flow_from_RSUPDINFO()
        self._t.update_flow_from_RSTRAN()
        self._t.update_flow_from_RSIOSMAP()
        self._t.update_flow_from_RSBSPOKE()
        self._t.update_flow_from_RSBOHDEST()
        self._t.update_flow_from_RSLDPSEL()
        self._t.update_flow_from_RSDCUBEMULTI()
        self._t.update_flow_from_RSRREPDIR()
        self.progressBar.setValue(i+1)
        self.progressBar.setVisible(False)
        self.statusbar.showMessage("Db tables regenerated.",10000)
        #Populate combo box of possible starting nodes for mapping
        for node in self._t.get_nodes(): self.map_startpoint_combo.addItem(node)

    def exit(self):
        sys.exit(0)

if __name__ == '__main__':
    import sys
    app = QApplication(sys.argv)
    MainWindow = QMainWindow()
    ui = BWMappingUI(MainWindow)
    MainWindow.show()
    sys.exit(app.exec_())
#TODO Option to display inactive (why does ZOHCRM003 show which has inactive update rule).
#TODO Populate node listbox after initial db regeneration
#TODO correct icons on error message boxwes
#TODO map connectivity and start node irrelevant for full map
#TODO Exit button does no work in cx_freeze distributed version