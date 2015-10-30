__author__ = 'jonathan.waterhouse@gmail.com'
from sqlite3 import OperationalError
import PyQt4.QtGui
from BWMapping import Ui_BWMapping
from BWFlowRFC_Table import BWFlowTable
from SVGDisplay import *
import os
import pickle

class BWMappingUI(Ui_BWMapping):
    def __init__(self,MainWindow):
        self.setupUi(MainWindow)
        self._otherGuiSetup()
        #Setup some internally required file locations
        dataDir = self.getDataDir() + os.sep
        self._iniFile = dataDir + "BWMapping.ini"
        self._files= {}
        # Read the ini file containing the graphviz executable location
        try:
            f = open(self._iniFile,'rb')
        except (IOError):
            self._files["DOT"] = ''
            f = open(self._iniFile,'wb')
            pickle.dump(self._files,f)
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
        w = PyQt4.QtGui.QWidget()
        dotLoc = str(PyQt4.QtGui.QFileDialog.getOpenFileName(w,"Select dot executable file"))
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
        self.fileRegenerate_Database.triggered.connect(self.generate_flow_Db)
        self.fileGenerate_User_Activity.triggered.connect(self.generate_user_activity)
        self.fileGenerate_Excel_Stats.triggered.connect(self.generate_excel_stats)
        self.actionLocate_dot.triggered.connect(self.locate_dot)
        self.exitButton.clicked.connect(self.exit)
        self.map_startpoint_combo.activated.connect(self.locate_node_by_index)

        path = ''
        database = path + 'BWStructure.db'
        self._flow_table = BWFlowTable(database)
        self._graph_file = "BWGraph.svg"
        self._mini_graph_file = "BWMiniGraph.svg"

        # Populate combo box
        self.map_connectivity_combo.addItems(['Forward','Backward', 'Forward & Backward','All Connections'])
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

    def locate_node_by_index(self,idx):
        if self.map_startpoint_combo.currentIndex() != -1:
            node = str(self.map_startpoint_combo.currentText())
            if self._flow_table.get_node(node):
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
                graphviz_iterable = self._flow_table.create_full_graph()
                self.statusbar.showMessage("Graph created in " + svg_file,10000)

            else: # Generate partial map

                svg_file = self._mini_graph_file
                start_node = str(self.map_startpoint_combo.currentText())
                direction = str(self.map_connectivity_combo.currentText())

                #Check start node exists
                if not self._flow_table.get_node(start_node):
                    self.statusbar.showMessage(start_node + " does not exist in BW Map.", 0)
                    return
                else: self.statusbar.showMessage("", 0)

                self.statusbar.showMessage("Creating graph in " + svg_file,10000)
                #Get correct graph connection mode
                if direction == 'Forward' :
                    forward = True
                    graphviz_iterable = self._flow_table.create_mini_graph_2(start_node,forward, show_queries)
                elif direction == 'Backward' :
                    forward = False
                    graphviz_iterable = self._flow_table.create_mini_graph_2(start_node,forward, show_queries)
                elif direction == 'Forward & Backward' :
                    forward = True
                    graphviz_iterable_1 = self._flow_table.create_mini_graph_2(start_node,forward, show_queries)
                    forward = False
                    graphviz_iterable_2 = self._flow_table.create_mini_graph_2(start_node,forward, show_queries)
                    graphviz_iterable = self._flow_table.add_graphviz_iterables(graphviz_iterable_1, graphviz_iterable_2)
                elif direction == 'All Connections' :
                    graphviz_iterable = self._flow_table.create_mini_graph_connections(start_node, show_queries)
                else: print ('Invalid mapping direction supplied')
        except (OperationalError):
            msg = PyQt4.QtGui.QMessageBox()
            msg.setText("ERROR")
            msg.setIcon(PyQt4.QtGui.QMessageBox.Critical)
            msg.setInformativeText("There is no database configured. Please use command - File\Regenerate Db")
            msg.exec_()
            self.statusbar.showMessage("There is no database configured. Please use command - File\Regenerate Db")
            return
        #Add decorations
        if 'BI7 Converted' in extras:
            graphviz_iterable = self._flow_table.decorate_graph_BI7Flow(graphviz_iterable)
            self.statusbar.showMessage("Generating BI7 conversion information",10000)
        if 'Flow Volumes last Run' in extras:
            graphviz_iterable = self._flow_table.decorate_graph_flow_volumes(graphviz_iterable)
            self.statusbar.showMessage("Generating flow volumes information",10000)
        if 'Stored Record Count' in extras:
            graphviz_iterable = self._flow_table.decorate_graph_sizes(graphviz_iterable)
            self.statusbar.showMessage("Generating data store size information",10000)

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

        self._flow_table.create_svg_file(graphviz_iterable,svg_file,dot_exec_loc)
        self.statusbar.showMessage("Graph created in " + svg_file,10000)

        dataDir = os.getcwd() + os.sep
        SVGDisplay(self, dataDir + svg_file, self._flow_table)
        return

    def generate_flow_Db(self):
        #Show progress since this is a long running operation
        i=0
        max_status_bar_value = len(self._flow_table.tab_spec) + 1
        self.progressBar.setMinimum(i)
        self.progressBar.setMaximum(max_status_bar_value)
        self.progressBar.setVisible(True)
        #Update tables from SAP
        self.statusbar.showMessage("Regenerating.",0)
        try:
            self._flow_table.get_SAP_table_via_RFC(self.statusbar,self.progressBar,i)

        except IOError:
            msg = PyQt4.QtGui.QMessageBox()
            msg.setText("ERROR")
            msg.setIcon(PyQt4.QtGui.QMessageBox.Critical)
            msg.setInformativeText("File " + file + " cannot be found.")
            msg.exec_()
            self.progressBar.setVisible(False)
            self.statusbar.showMessage("Error populating database.",0)
            return # on error
        #Update flow table
        self.statusbar.showMessage("Regenerating DATAFLOW table",0)
        self._flow_table.create_flow_table()
        self._flow_table.update_text_table()
        self._flow_table.update_flow_from_RSUPDINFO()
        self.statusbar.showMessage(".....from RSTRAN",0)
        self._flow_table.update_flow_from_RSTRAN()
        self.statusbar.showMessage(".....from RSIOSMAP",0)
        self._flow_table.update_flow_from_RSIOSMAP()
        self.statusbar.showMessage(".....from RSBSPOKE",0)
        self._flow_table.update_flow_from_RSBSPOKE()
        self.statusbar.showMessage(".....from RSBOHDEST",0)
        self._flow_table.update_flow_from_RSBOHDEST()
        self.statusbar.showMessage(".....from RSLDPSEL",0)
        self._flow_table.update_flow_from_RSLDPSEL()
        self.statusbar.showMessage(".....from RSDCUBEMULTI",0)
        self._flow_table.update_flow_from_RSDCUBEMULTI()
        self.statusbar.showMessage(".....from RSRREPDIR",0)
        self._flow_table.update_flow_from_RSRREPDIR()
        self.statusbar.showMessage(".....from RSQTOBJ",0)
        self._flow_table.update_flow_from_RSQTOBJ()
        self.progressBar.setValue(max_status_bar_value)
        self.progressBar.setVisible(False)
        self.statusbar.showMessage("Db tables regenerated.",10000)
        #Populate combo box of possible starting nodes for mapping
        for node in self._flow_table.get_nodes(): self.map_startpoint_combo.addItem(node)

    def generate_user_activity(self):
        self._flow_table.get_user_activity_LISTCUBE_via_RFC()

    def generate_excel_stats(self):
        workbook_name = 'BWStatistics.xlsx'
        self.statusbar.showMessage("Generating Excel file.",10000)
        self._flow_table.create_BW_stats(workbook_name)
        self.statusbar.showMessage("Excel file " + workbook_name + " created.",10000)

    def exit(self):
        sys.exit(0)

if __name__ == '__main__':
    import sys
    app = PyQt4.QtGui.QApplication(sys.argv)
    MainWindow = PyQt4.QtGui.QMainWindow()
    ui = BWMappingUI(MainWindow)
    MainWindow.show()
    sys.exit(app.exec_())
#TODO Option to display inactive (why does ZOHCRM003 show which has inactive update rule).
#TODO GUI does not update during heavy processing (use threads?)
#TODO Some mechanism to identify a flow associated with a process area eg CMIS which has many disconnected flows
#TODO     This could be as simple as populating datastore names in the dropdown list box (and allowing sorting by text)
#TODO     to allow easy identification of relevant datastores. Alternatively some sort of wider map display that is
#TODO     not everything