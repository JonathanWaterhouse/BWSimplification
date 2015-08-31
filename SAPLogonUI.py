import pyrfc
from SAPLogon import Ui_Dialog
import PyQt4.QtGui
import PyQt4.QtCore
import PyQt4.Qt
import sys
__author__ = 'U104675'


class SAPLogonUI(Ui_Dialog):
    def __init__(self):
        """
        Create a GUI to allow supply of SAP login credentials
        :return: None
        """
        self._dlg = PyQt4.QtGui.QDialog()
        self.setupUi(self._dlg)
        self.cancel_pushButton.clicked.connect(self._exit)
        self.OK_pushButton.clicked.connect(self._login)
        self._dlg.setVisible(True)
        self._dlg.exec_()

    def _exit(self):
        sys.exit(0)

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
        self._SAPConn = pyrfc.Connection(**params)
        self._dlg.close()

    def get_SAP_connection(self):
        return self._SAPConn
    #TODO Cancel has to stop the whole app. Figure out how to get back to main window.