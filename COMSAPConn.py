from datetime import *
import sqlite3
import pyrfc
from SAPLogonUI import *
__author__ = 'U104675'

class SAP_to_sqlite_table():

    def __init__(self):
        self._FM = 'RFC_READ_TABLE'

    def login_to_SAP(self):
        """
        Login to SAP. To do this a simple UI is presented to allow user input of credentials and target system. This
        avoids any hard coding of credentials and flexibility as to target system eg dev, QA, Prod.
        :return: None. Sets a class connection object used by other methods.
        """
        """
        :return:
        """
        login = SAPLogonUI()
        self._SAP_conn = login.get_SAP_connection() #SAP Connection Object

    def close_SAP_connection(self):
        self._SAP_conn.close()

    def read_SAP_table(self, table, maxrows, skip_rows, selection, fields, retrieve_data):
        """
        Return selected data from a table using SAP FM RFC_READ_TABLE. Python relies on SAP pyrfc module to do this
        and installation of nwrfcsdk from SAP. This is described here http://sap.github.io/PyRFC/install.html
        :param table: Table to read
        :param maxrows: max number of rows to process there may be a limit on what RFC_READ_TABLE can process
        :param selection: A list of dictionaries of SAP open sql statements to limit the extraction.
            Example format : selection = [{'TEXT' : "DTA = 'ZO00014'"}]
        :param fields: A List of dictionaries of the required fields for retrieval.
        Example format:  fields = [{'FIELDNAME': 'DTA'}, {'FIELDNAME':'DTA_TYPE'}]
        :param retrieve_data: If blank then data is retrieved, if not only record definitions are retrieved. 1 character.
        :return: The FRC_READ_TABLE FM returns a dictionary with 3 entries, keyed on values "DATA", "FIELDS", "OPTIONS"
            "OPTIONS" specifies the options that were chosen in the selection. It is a list of dictionaries in the same
            format as the "selection" parameter described earlier

            "FIELDS" gives a fields spec for each field returned. It only returns specs for fields requested in the "fields"
            option described above. The format is a list of dictionaries each one having format like
            {u'FIELDTEXT': u'Data Source or Target for Status Manager', u'TYPE': u'C', u'LENGTH': u'000045', u'FIELDNAME': u'DTA', u'OFFSET': u'000000'}

            "DATA" is the returned data payload and is a list of dictionaries (one per record returned) formatted like
            {u'WA': u'ZO00014                                      ODSO  201208142012081405262..........}
            In order to know which part of he record contains a given field you need to use the "FIELDS" output to get
            field positions in the record.
        """
        #Get incoming field list into correct format for RFC call [{'FIELDNAME': 'DTA'}, {'FIELDNAME':'DTA_TYPE'}]
        SAP_fld_spec = []
        for fld in fields: SAP_fld_spec.append(dict(FIELDNAME = fld))

        result = self._SAP_conn.call(self._FM, QUERY_TABLE = table, NO_DATA = retrieve_data, ROWCOUNT = maxrows,
                                     ROWSKIPS = skip_rows,  OPTIONS =  selection, FIELDS = SAP_fld_spec)
        #Test statements
        #print result['FIELDS']
        #print result['DATA']
        return result

    def update_sqlite_table(self, sqlite_db_name, table, append, RFC_READ_TABLE_input):
        """
        Takes the output from SAP FM FRC_READ_TABLE and adds to table in sqlite database whose name is passed. If it is
        the first write to the table for this program call then the table (it it exists) is dropped, and then created
        afresh The table specifications including fieldnames and lengths are all taken from the output of the SAP FM.
        This depends upon the fact the the FM only returns the field specifications for the fields requested in the
        function call
        :param sqlite_db_name: database name as known to windows eg database.db
        :param table: table to create and populate in database.db
        :param append: A boolean indicating whether we are to process a further set of records for a table or if it is
                the first set. In the latter case we drop the table first.
        :param RFC_READ_TABLE_input: the output of RFC_READ_TABLE having format defined in method read_SAP_table
        :return: nothing. Result of running tis method is a sqlite db with newly filled table.
        """
        RFC_out = RFC_READ_TABLE_input
        #Create the table in local sqlite database
        sqlite_conn = sqlite3.connect(sqlite_db_name)
        c = sqlite_conn.cursor()
        #Delete table if already exists
        if not append:
            c.execute('DROP TABLE IF EXISTS ' + table)
            #Define table structure from the SAP "FIELDS" Information
            sql_stmt_field_defs = []
            for field in RFC_out['FIELDS']:
                sql_stmt_field_defs.append(field['FIELDNAME'] + ' CHAR(' + field['LENGTH'].lstrip('0') + ')')
            sql_stmt = 'CREATE TABLE ' + table + ' (' + ','.join(sql_stmt_field_defs) + ')'
            c.execute(sql_stmt)

        ##Now load the data
        #Create a list of tuples of start and ends of fields in SAP supplied records
        field_boundaries = []
        i = 0
        for field in RFC_out['FIELDS']:
            field_boundaries.append((i, i+ int(field['LENGTH']))) #A tuple
            i += int(field['LENGTH'])

        #Process the SAP datarecords, defining fields according to field boundaries
        record_tup = [] #List of 2-tuples
        payload = ''
        for record in RFC_out['DATA']:
            payload = record['WA']
            record_tup.append(tuple([payload[bound[0]: bound[1]].strip() for bound in field_boundaries]))

        #Build sql to post to the sqlite database table
        sql_stmt_field_defs = []
        for field in RFC_out['FIELDS']:
            sql_stmt_field_defs.append(field['FIELDNAME'])
        val_qmarks = ['?' for el in sql_stmt_field_defs]
        sql_stmt = 'INSERT INTO ' + table + ' (' + ','.join(sql_stmt_field_defs) + ') VALUES (' + ','.join(val_qmarks) + ')'

        c.executemany(sql_stmt,record_tup)

        #We are done. Commit and close sqlite connection
        sqlite_conn.commit()
        sqlite_conn.close()

