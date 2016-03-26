import pyrfc
from SAPLogon import Ui_Dialog
import PyQt4.QtGui
import PyQt4.QtCore
import PyQt4.Qt
import sys
__author__ = 'U104675'


class SAPLogonUI(Ui_Dialog):
    def __init__(self,parent):
        """
        Create a GUI to allow supply of SAP login credentials
        :return: None
        """
        self._dlg = PyQt4.QtGui.QDialog()
        self.setupUi(self._dlg)
        self._parent = parent
        self.cancel_pushButton.clicked.connect(self._exit)
        self.OK_pushButton.clicked.connect(self._login)
        self._dlg.setVisible(True)
        self._dlg.exec_()

    def _exit(self):
        """
        Close the dialog without having logged in so call the parent method and pass a "None" connection object.
        :return: Nothing
        """
        self._parent.set_SAP_connection(None)
        self._dlg.close()

    def _login(self):
        client = str(self.client_lineEdit.text())
        user = str(self.user_lineEdit.text())
        passwd = str(self.password_lineEdit.text())
        lang = str(self.langu_lineEdit.text())
        mshost = str(self.msg_server_lineEdit.text())
        sysid = str(self.system_lineEdit.text())
        group = str(self.group_lineEdit.text())
        params = {'client' : client, 'user' : user, 'passwd' : passwd , 'lang' : lang,
                  'mshost' : mshost, 'sysid' : sysid, 'group' : group}
        try:
            self._SAPConn = pyrfc.Connection(**params)
            self._parent.set_SAP_connection(self._SAPConn)  # Pass connection back to parent
            self._dlg.close()
            return
        except Exception, message:
            msg = PyQt4.QtGui.QMessageBox()
            msg.setWindowTitle("Error")
            msg.setIcon(PyQt4.QtGui.QMessageBox.Critical)
            msg.setInformativeText("RFC Error : " + repr(message))
            msg.exec_()
            self._parent.set_SAP_connection(None)
            return

    def get_SAP_connection(self):
        return self._SAPConn
    #TODO Cancel has to stop the whole app. Figure out how to get back to main window.