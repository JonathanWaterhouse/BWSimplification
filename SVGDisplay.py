from PyQt4.QtCore import QTimer, QString, Qt
from PyQt4.QtGui import QCursor, QDialog, QLabel, QTextFormat
from PyQt4 import QtCore, QtGui
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
        dlg = QDialog()
        self.setupUi(dlg)
        self.imageUrl = QtCore.QUrl.fromLocalFile(svgFile) # Fully qualified filename
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
        label = QLabel(name)
        label.textFormat = Qt.RichText
        label.alignment = Qt.AlignLeft
        label.move(QCursor.pos().x()-40,QCursor.pos().y()+20)
        label.setWindowFlags(QtCore.Qt.SplashScreen)
        label.show()
        timer = QTimer()
        timer.setSingleShot(True)
        timer.connect(label,timer.timeout(),label.destroy(True))
        timer.start(1000)


