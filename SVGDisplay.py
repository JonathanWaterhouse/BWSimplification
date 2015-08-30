import PyQt4.QtGui
import PyQt4.QtCore
import PyQt4.Qt
#import PyQt4.QtGui.QTimer, PyQt4.QtGui.QString, PyQt4.QtGui.Qt
#import PyQt4.QtGui.QCursor, PyQt4.QtGui.QDialog, PyQt4.QtGui.QLabel, PyQt4.QtGui.QTextFormat
from SVGView import Ui_Dialog
#from PyQt4.QtNetwork import *
#from PyQt4.QtWebKit import *
#from PyQt4.QtPrintSupport import *
__author__ = 'jonathan.waterhouse@gmail.com'

class SVGDisplay(Ui_Dialog):
    """
    Create a small webkit based box to display schedule diagrams
    A test comment to test git branching
    """
    def __init__(self,parent,svgFile, database):
        """
        Create the display box based on input parent widget and populated with
        """
        self._db = database
        dlg = PyQt4.QtGui.QDialog()
        self.setupUi(dlg)
        self.imageUrl = PyQt4.QtCore.QUrl.fromLocalFile(svgFile) # Fully qualified filename
        self.webView.load(self.imageUrl)
        self.webView.setZoomFactor(0.5)
        self.horizontalSlider.setMinimum(0)
        self.horizontalSlider.setMaximum(100)
        self.horizontalSlider.setValue(50)
        self.horizontalSlider.setTracking(True)
        self.horizontalSlider.valueChanged.connect(self.magnification)
        self.webView.selectionChanged.connect(self.showDetails)
        dlg.setVisible(True)
        dlg.exec_()

    def magnification(self):
        self.webView.setZoomFactor(self.horizontalSlider.sliderPosition()/100.0)

    def showDetails(self):
        try:
            key = str(self.webView.selectedText())
            if key != '': name = self._db.get_node_text(key)
            else: name = ''
        except KeyError: return
        label = PyQt4.QtGui.QLabel(name)
        #label.setTextFormat(PyQt4.Qt.QtCore.RichText)
        #label.alignment = PyQt4.Qt.AlignLeft
        label.move(PyQt4.QtGui.QCursor.pos().x()-40,PyQt4.QtGui.QCursor.pos().y()+20)
        label.setWindowFlags(PyQt4.QtCore.Qt.SplashScreen)
        label.show()
        timer = PyQt4.QtCore.QTimer()
        timer.setSingleShot(True)
        timer.connect(label,timer.timeout(),label.destroy(True))
        timer.start(1000)


