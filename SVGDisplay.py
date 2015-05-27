from PyQt5.QtCore import QTimer
from PyQt5.QtGui import QCursor
from PyQt5.QtWidgets import QLabel
from PyQt5 import QtCore, QtGui, QtWidgets
from SVGView import Ui_Dialog
from PyQt5.QtNetwork import *
from PyQt5.QtWebKit import *
from PyQt5.QtPrintSupport import *
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
        dlg = QtWidgets.QDialog()
        self.setupUi(dlg)
        imageUrl = QtCore.QUrl.fromLocalFile(svgFile) # Fully qualified filename
        self.webView.load(imageUrl)
        self.webView.setZoomFactor(0.5)
        self.horizontalSlider.setValue(50)
        self.horizontalSlider.valueChanged.connect(self.magnification)
        self.webView.selectionChanged.connect(self.showDetails)
        dlg.setVisible(True)
        dlg.exec_()

    def magnification(self):
        self.webView.setZoomFactor(self.horizontalSlider.sliderPosition()/100)

    def showDetails(self):
        try:
            key = self.webView.selectedText()
            if key != '': name = self._db.get_node_text(key)
            else: name = ''
        except KeyError: return
        label = QLabel('<font style="color: grey; background-color: yellow"><p>' + repr(name) + '</p></font>')
        label.move(QCursor.pos().x()+30,QCursor.pos().y()+20)
        label.setWindowFlags(QtCore.Qt.SplashScreen)
        label.show()
        QTimer.singleShot(10000,label.destroy)

