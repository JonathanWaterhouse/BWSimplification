import PyQt4.QtGui
import PyQt4.QtCore
import PyQt4.Qt
from PyQt4.QtGui import QDialog
from SVGView import Ui_Dialog
from BWMapRFC_UI import BWMappingUI

__author__ = 'jonathan.waterhouse@gmail.com'

class SVGDisplay(QDialog,Ui_Dialog):
    """
    Create a small webkit based box to display schedule diagrams
    A test comment to test git branching
    """
    def __init__(self,parent,svgFile, database):
        """
        Create the display box based on input parent widget and populated with
        """
        self._db = database
        #dlg = PyQt4.QtGui.QDialog()
        QDialog.__init__(self)
        #self.setupUi(dlg)
        self._parent = parent
        self.setupUi(self)
        self.imageUrl = PyQt4.QtCore.QUrl.fromLocalFile(svgFile) # Fully qualified filename
        self.webView.load(self.imageUrl)
        self.webView.setZoomFactor(0.5)
        self.horizontalSlider.setMinimum(0)
        self.horizontalSlider.setMaximum(100)
        self.horizontalSlider.setValue(50)
        self.horizontalSlider.setTracking(True)
        self.horizontalSlider.valueChanged.connect(self.magnification)
        self.webView.selectionChanged.connect(self.showDetails)
        self.search_lineEdit.returnPressed.connect(self.find)
        self.addNode_pushButton.clicked.connect(self.update_diagram)
        #dlg.setVisible(True)
        #dlg.exec_()
        self.setVisible(True)
        self.exec_()

    def magnification(self):
        self.webView.setZoomFactor(self.horizontalSlider.sliderPosition()/100.0)

    def showDetails(self):
        try:
            key = str(self.webView.selectedText())
            self.addNodeName_label.setText(key)
            if key != '': name = self._db.get_node_text(key)
            else: name = ''
        except KeyError: return
        self.label = PyQt4.QtGui.QLabel(name)
        self.label.move(PyQt4.QtGui.QCursor.pos().x()-40,PyQt4.QtGui.QCursor.pos().y()+20)
        self.label.setWindowFlags(PyQt4.QtCore.Qt.SplashScreen)
        self.label.show()
        timer = PyQt4.QtCore.QTimer()
        timer.setSingleShot(True)
        timer.setInterval(10)
        timer.timeout.connect(self.destroy_label)
        timer.start()

    def destroy_label(self):
        self.label.destroy(True)
        return

    def find(self):
        text = self.search_lineEdit.text()
        self.webView.findText(text,PyQt4.QtWebKit.QWebPage.FindWrapsAroundDocument)
        return

    def update_diagram(self):
        """
        The intention of this method is to create an even which may be picked up in the main
        GUI program and used to extend the currently displayed map with the dependencies of other
        nodes
        """
        PyQt4.QtCore.QObject.connect(self, PyQt4.QtCore.SIGNAL("update_node"), self._parent.add_node)
        self.emit(PyQt4.QtCore.SIGNAL('update_node'), self.addNodeName_label.text())
        return