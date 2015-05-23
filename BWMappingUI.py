from PyQt5.QtWidgets import QApplication, QMainWindow
from BWMapping import Ui_BWMapping
from BWFlowTable import BWFlowTable
from SVGDisplay import *
import os

__author__ = 'U104675'
import BWMapping
class BWMappingUI(Ui_BWMapping):
    def __init__(self,MainWindow):
        self.setupUi(MainWindow)
        self._otherGuiSetup()

    def _otherGuiSetup(self):
        """ Do other setup things required to get the static GUI components set up, and
        components acting correctly upon user interaction. There should NOT be any
        code associated with date reading or population in here.
        """
        #Actions
        self.generate_pushButton.clicked.connect(self.generateMap)
        self.fileRegenerate_Database.triggered.connect(self.generateDb)
        self.exitButton.clicked.connect(self.exit)

        path = ''
        database = path + 'BWStructure.db'
        self._t = BWFlowTable(database)
        self._graph_file = "BWGraph.svg"
        self._mini_graph_file = "BWMiniGraph.svg"

        # Populate combo boxes
        for node in self._t.get_nodes(): self.map_startpoint_combo.addItem(node)
        self.map_startpoint_combo.setCurrentIndex(0)
        self.map_connectivity_combo.addItems(['Forward','Backward', 'Forward & Backward','All Connections'])

        self.progressBar.setVisible(False)

        # location of text files to populate sqlite tables
        fin = "RSLDPSEL.txt" # This file has one line split over two
        fout = "RSLDPSELUnsplit.txt" # After joing the two line halves this is the new filename
        sep = "|" # fieldseparator in the file
        self._t._unsplit(fin, fout, sep) #this method does the unsplitting
        self._table_file_map = dict(RSTRAN="RSTRAN.txt", RSISOSMAP="RSISOSMAP.txt", RSLDPSEL=fout, RSLDPIO="RSLDPIO.txt",
                              RSLDPIOT="RSLDPIOT.txt", RSBSPOKE="RSBSPOKE.txt", RSBOHDEST="RSBOHDEST.txt",
                              RSIS="RSIS.txt", RSUPDINFO="RSUPDINFO.txt", RSDCUBEMULTI='RSDCUBEMULTI.txt',
                              RSRREPDIR='RSRREPDIR.txt', RSDCUBET='RSDCUBET.txt', RSZELTDIR='RSZELTDIR.txt',
                              RSMDATASTATE_EXT='RSMDATASTATE_EXT.txt', RSSTATMANPART='RSSTATMANPART.txt')

    def generateMap(self):
        svg_file = ''
        extras = []
        if self.BI7_checkBox.isChecked(): extras.append('BI7 Converted')
        if self.flowVol_checkBox.isChecked(): extras.append('Flow Volumes last Run')
        if self.storedRec_checkBox.isChecked(): extras.append('Stored Record Count')

        graphviz_iterable = None
        if self.fullmap_radio.isChecked(): # generate full map
            svg_file = self._graph_file
            self.statusbar.showMessage("Creating graph in " + svg_file,0)
            graphviz_iterable = self._t.create_full_graph()
            self.statusbar.showMessage("Graph created in " + svg_file,10000)
        else: # Generate partial map
            svg_file = self._mini_graph_file
            start_node = self.map_startpoint_combo.currentText()
            direction = self.map_connectivity_combo.currentText()

            self.statusbar.showMessage("Creating graph in " + svg_file,10000)
            #Get correct graph connection mode
            if direction == 'Forward' :
                log_dir = True
                graphviz_iterable = self._t.create_mini_graph(start_node,log_dir)
            elif direction == 'Backward' :
                log_dir = False
                graphviz_iterable = self._t.create_mini_graph(start_node,log_dir)
            elif direction == 'Forward & Backward' :
                graphviz_iterable = self._t.create_mini_graph_bwd_fwd(start_node)
            elif direction == 'All Connections' :
                graphviz_iterable = self._t.create_mini_graph_connections(start_node)
            else: print ('Invalid mapping direction supplied')

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

        self._t.create_svg_file(graphviz_iterable,svg_file)
        self.statusbar.showMessage("Graph created in " + svg_file,10000)

        dataDir = os.getcwd() + os.sep
        SVGDisplay(self, dataDir + svg_file)
        #TODO Add ability to touch a datastore and get its name
        #TODO nice icon for svgdisplay
        return

    def generateDb(self):
        unwanted_cols = []
        #Show progress since this is a long running operation
        i=0
        self.progressBar.setMinimum(i)
        self.progressBar.setMaximum(len(self._table_file_map)+ 1)
        self.progressBar.setVisible(True)
        #Update tables from SAP
        for table, file in self._table_file_map.items():
            self.statusbar.showMessage("Regenerating " + table,0)
            self._t.update_table({table : file}, unwanted_cols)
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

    def exit(self):
        exit(0)

if __name__ == '__main__':
    import sys
    app = QApplication(sys.argv)
    MainWindow = QMainWindow()
    ui = BWMappingUI(MainWindow)
    MainWindow.show()
    sys.exit(app.exec_())

