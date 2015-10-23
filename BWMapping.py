# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'BWMapping.ui'
#
# Created by: PyQt4 UI code generator 4.11.4
#
# WARNING! All changes made in this file will be lost!

from PyQt4 import QtCore, QtGui

try:
    _fromUtf8 = QtCore.QString.fromUtf8
except AttributeError:
    def _fromUtf8(s):
        return s

try:
    _encoding = QtGui.QApplication.UnicodeUTF8
    def _translate(context, text, disambig):
        return QtGui.QApplication.translate(context, text, disambig, _encoding)
except AttributeError:
    def _translate(context, text, disambig):
        return QtGui.QApplication.translate(context, text, disambig)

class Ui_BWMapping(object):
    def setupUi(self, BWMapping):
        BWMapping.setObjectName(_fromUtf8("BWMapping"))
        BWMapping.resize(518, 374)
        sizePolicy = QtGui.QSizePolicy(QtGui.QSizePolicy.Fixed, QtGui.QSizePolicy.Fixed)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(BWMapping.sizePolicy().hasHeightForWidth())
        BWMapping.setSizePolicy(sizePolicy)
        icon = QtGui.QIcon()
        icon.addPixmap(QtGui.QPixmap(_fromUtf8("thread_16xLG.png")), QtGui.QIcon.Normal, QtGui.QIcon.Off)
        BWMapping.setWindowIcon(icon)
        self.centralwidget = QtGui.QWidget(BWMapping)
        self.centralwidget.setObjectName(_fromUtf8("centralwidget"))
        self.groupBox_2 = QtGui.QGroupBox(self.centralwidget)
        self.groupBox_2.setGeometry(QtCore.QRect(30, 20, 121, 81))
        self.groupBox_2.setObjectName(_fromUtf8("groupBox_2"))
        self.partialmap_radio = QtGui.QRadioButton(self.groupBox_2)
        self.partialmap_radio.setGeometry(QtCore.QRect(20, 20, 82, 31))
        self.partialmap_radio.setChecked(True)
        self.partialmap_radio.setObjectName(_fromUtf8("partialmap_radio"))
        self.fullmap_radio = QtGui.QRadioButton(self.groupBox_2)
        self.fullmap_radio.setGeometry(QtCore.QRect(20, 50, 82, 17))
        self.fullmap_radio.setObjectName(_fromUtf8("fullmap_radio"))
        self.map_connectivity_combo = QtGui.QComboBox(self.centralwidget)
        self.map_connectivity_combo.setGeometry(QtCore.QRect(30, 140, 151, 22))
        self.map_connectivity_combo.setObjectName(_fromUtf8("map_connectivity_combo"))
        self.map_startpoint_combo = QtGui.QComboBox(self.centralwidget)
        self.map_startpoint_combo.setGeometry(QtCore.QRect(240, 140, 241, 22))
        self.map_startpoint_combo.setEditable(True)
        self.map_startpoint_combo.setInsertPolicy(QtGui.QComboBox.InsertAlphabetically)
        self.map_startpoint_combo.setObjectName(_fromUtf8("map_startpoint_combo"))
        self.label = QtGui.QLabel(self.centralwidget)
        self.label.setGeometry(QtCore.QRect(30, 110, 91, 16))
        self.label.setObjectName(_fromUtf8("label"))
        self.label_2 = QtGui.QLabel(self.centralwidget)
        self.label_2.setGeometry(QtCore.QRect(240, 110, 181, 16))
        self.label_2.setObjectName(_fromUtf8("label_2"))
        self.label_3 = QtGui.QLabel(self.centralwidget)
        self.label_3.setGeometry(QtCore.QRect(30, 180, 91, 16))
        self.label_3.setObjectName(_fromUtf8("label_3"))
        self.generate_pushButton = QtGui.QPushButton(self.centralwidget)
        self.generate_pushButton.setGeometry(QtCore.QRect(400, 210, 75, 23))
        self.generate_pushButton.setObjectName(_fromUtf8("generate_pushButton"))
        self.progressBar = QtGui.QProgressBar(self.centralwidget)
        self.progressBar.setGeometry(QtCore.QRect(400, 290, 118, 23))
        self.progressBar.setProperty("value", 0)
        self.progressBar.setObjectName(_fromUtf8("progressBar"))
        self.exitButton = QtGui.QPushButton(self.centralwidget)
        self.exitButton.setGeometry(QtCore.QRect(400, 250, 75, 23))
        self.exitButton.setObjectName(_fromUtf8("exitButton"))
        self.BI7_checkBox = QtGui.QCheckBox(self.centralwidget)
        self.BI7_checkBox.setGeometry(QtCore.QRect(30, 200, 171, 17))
        self.BI7_checkBox.setObjectName(_fromUtf8("BI7_checkBox"))
        self.flowVol_checkBox = QtGui.QCheckBox(self.centralwidget)
        self.flowVol_checkBox.setGeometry(QtCore.QRect(30, 220, 181, 17))
        self.flowVol_checkBox.setObjectName(_fromUtf8("flowVol_checkBox"))
        self.storedRec_checkBox = QtGui.QCheckBox(self.centralwidget)
        self.storedRec_checkBox.setGeometry(QtCore.QRect(30, 240, 171, 17))
        self.storedRec_checkBox.setObjectName(_fromUtf8("storedRec_checkBox"))
        self.queries_checkBox = QtGui.QCheckBox(self.centralwidget)
        self.queries_checkBox.setGeometry(QtCore.QRect(30, 260, 151, 17))
        self.queries_checkBox.setObjectName(_fromUtf8("queries_checkBox"))
        BWMapping.setCentralWidget(self.centralwidget)
        self.menubar = QtGui.QMenuBar(BWMapping)
        self.menubar.setGeometry(QtCore.QRect(0, 0, 518, 21))
        self.menubar.setObjectName(_fromUtf8("menubar"))
        self.menuFile = QtGui.QMenu(self.menubar)
        self.menuFile.setObjectName(_fromUtf8("menuFile"))
        BWMapping.setMenuBar(self.menubar)
        self.statusbar = QtGui.QStatusBar(BWMapping)
        self.statusbar.setObjectName(_fromUtf8("statusbar"))
        BWMapping.setStatusBar(self.statusbar)
        self.fileRegenerate_Database = QtGui.QAction(BWMapping)
        self.fileRegenerate_Database.setObjectName(_fromUtf8("fileRegenerate_Database"))
        self.actionLocate_dot = QtGui.QAction(BWMapping)
        self.actionLocate_dot.setObjectName(_fromUtf8("actionLocate_dot"))
        self.fileGenerate_User_Activity = QtGui.QAction(BWMapping)
        self.fileGenerate_User_Activity.setObjectName(_fromUtf8("fileGenerate_User_Activity"))
        self.fileGenerate_Excel_Stats = QtGui.QAction(BWMapping)
        self.fileGenerate_Excel_Stats.setObjectName(_fromUtf8("fileGenerate_Excel_Stats"))
        self.menuFile.addAction(self.fileRegenerate_Database)
        self.menuFile.addAction(self.fileGenerate_User_Activity)
        self.menuFile.addAction(self.fileGenerate_Excel_Stats)
        self.menuFile.addAction(self.actionLocate_dot)
        self.menubar.addAction(self.menuFile.menuAction())

        self.retranslateUi(BWMapping)
        QtCore.QMetaObject.connectSlotsByName(BWMapping)

    def retranslateUi(self, BWMapping):
        BWMapping.setWindowTitle(_translate("BWMapping", "BW Mapping Tool", None))
        self.groupBox_2.setTitle(_translate("BWMapping", "Map Type", None))
        self.partialmap_radio.setText(_translate("BWMapping", "Partial", None))
        self.fullmap_radio.setText(_translate("BWMapping", "Full", None))
        self.label.setText(_translate("BWMapping", "Map Connectivity", None))
        self.label_2.setText(_translate("BWMapping", "Partial Map Starting Object", None))
        self.label_3.setText(_translate("BWMapping", "Extra Information:", None))
        self.generate_pushButton.setText(_translate("BWMapping", "Generate", None))
        self.exitButton.setText(_translate("BWMapping", "Exit", None))
        self.BI7_checkBox.setText(_translate("BWMapping", "BI7 Converted Flows", None))
        self.flowVol_checkBox.setText(_translate("BWMapping", "Flow Volumes for Last Run", None))
        self.storedRec_checkBox.setText(_translate("BWMapping", "Stored Record Count", None))
        self.queries_checkBox.setText(_translate("BWMapping", "Show Queries", None))
        self.menuFile.setTitle(_translate("BWMapping", "File", None))
        self.fileRegenerate_Database.setText(_translate("BWMapping", "Generate Flow Tables", None))
        self.actionLocate_dot.setText(_translate("BWMapping", "Locate \'dot\' Executable", None))
        self.fileGenerate_User_Activity.setText(_translate("BWMapping", "Generate User Activity", None))
        self.fileGenerate_Excel_Stats.setText(_translate("BWMapping", "Generate Excel Stats", None))

