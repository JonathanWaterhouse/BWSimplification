from __future__ import division
from __future__ import absolute_import
import subprocess
import os
import os.path
from io import open
from PyQt4.QtCore import QCoreApplication
from COMSAPConn import *
from collections import OrderedDict
import datetime

__author__ = u'jonathan.waterhouse@gmail.com'
import sys
import sqlite3

sys.path.append(os.path.dirname(os.getcwdu()))

class BWFlowTable(object):
    u"""
    This class is used to create a database of SAP tables, using pyRFC module from SAP to call FM
    RFC_READ_TABLE. The data is stored in a local sqlite3 database for local querying.

    The following tables are currently used
    RSIS - Infosource
    RSUPDINFO - Update Rules Infoprovider to infosource (Transactions and flexible Infosource?)
    RSTRAN - BI7 Data transformations
    RSISOSMAP (OLTP to BW Infosource Map) Active Only
    RSLDPSEL (OBJVERS = A, FILENAME = non blank, KIND = T,M ie master data/texts) - Selections in Infopackages;
        combined with info from RSLDPIO/RSLDPIOT keyed on LOGDPID For flat file loads
    RSBSPOKE (Active Only)
    RSBOHDEST (OBJVERS = A, NEW_OHD = X to exclude infospokes)
    RSDCUBEMULTI - Cube contents of multiproviders
    RSRREPDIR - Cubes and multiproviders and queries off them
    RSDCUBET - Cube and Multiprovider texts
    RSMDATASTATE_EXT - Record Counts
    USER_ADDRP - User names and Id's
    """
    def __init__(self, database):
        u"""
        :param database: sets the database name storing all the internal information
        :return: nothing
        """
        self._database = database
        self._max_rows = 75000 #Maximum rows we will attempt to pull from a table in one RFC call.
        #Next section defines the tables we want, the fields, any sql "WHERE" criteria and whetherw we want data or just
        #a table spec.
        self.tab_spec = OrderedDict(
            RSBOHDEST= {'FIELDS' :[],
                        'MAXROWS': self._max_rows,
                        'SELECTION' : [{'TEXT' : "OBJVERS = 'A'"}],
                        'RETRIEVEDATA' : ''},
            RSBOHDESTT= {'FIELDS' :[],
                        'MAXROWS': self._max_rows,
                        'SELECTION' : [{'TEXT' : "OBJVERS = 'A' AND LANGU = 'E'"}],
                        'RETRIEVEDATA' : ''},
            RSBSPOKE= {'FIELDS' :[],
                       'MAXROWS': self._max_rows,
                       'SELECTION' : [{'TEXT' : "OBJVERS = 'A'"}],
                       'RETRIEVEDATA' : ''},
            RSBSPOKET= {'FIELDS' :[],
                       'MAXROWS': self._max_rows,
                       'SELECTION' : [{'TEXT' : "OBJVERS = 'A' AND LANGU = 'E'"}],
                       'RETRIEVEDATA' : ''},
            RSDCUBEMULTI= {'FIELDS' :[],
                           'MAXROWS': self._max_rows,
                           'SELECTION' : [{'TEXT' : "OBJVERS = 'A'"}],
                           'RETRIEVEDATA' : ''},
            RSDCUBET= {'FIELDS' :[],
                       'MAXROWS': self._max_rows,
                       'SELECTION' : [{'TEXT' : "OBJVERS = 'A' AND LANGU = 'E'"}],
                       'RETRIEVEDATA' : ''},
            RSDODSOT= {'FIELDS' :[],
                       'MAXROWS': self._max_rows,
                       'SELECTION' : [{'TEXT' : "OBJVERS = 'A' AND LANGU = 'E'"}],
                       'RETRIEVEDATA' : ''},
            RSIS= {'FIELDS' :[],
                   'MAXROWS': self._max_rows,
                   'SELECTION' : [{'TEXT' : "OBJVERS = 'A'"}],
                   'RETRIEVEDATA' : ''},
            RSIST= {'FIELDS' :[],
                   'MAXROWS': self._max_rows,
                   'SELECTION' : [{'TEXT' : "OBJVERS = 'A' AND LANGU = 'E'"}],
                   'RETRIEVEDATA' : ''},
            RSISOSMAP= {'FIELDS' :[],
                        'MAXROWS': self._max_rows,
                        'SELECTION' : [{'TEXT' : "OBJVERS = 'A'"}],
                        'RETRIEVEDATA' : ''},
            RSLDPIO= {'FIELDS' :[],
                      'MAXROWS': self._max_rows,
                      'SELECTION' : [{'TEXT' : "OBJVERS = 'A'"}],
                      'RETRIEVEDATA' : ''},
            RSLDPIOT= {'FIELDS' :[],
                       'MAXROWS': self._max_rows,
                       'SELECTION' : [{'TEXT' : "OBJVERS = 'A' AND LANGU = 'E'"}],
                       'RETRIEVEDATA' : ''},
            RSLDPSEL= {'FIELDS' :['LOGDPID', 'OBJVERS', 'KIND', 'FILENAME'],
                       'MAXROWS': self._max_rows,
                       'SELECTION' : [{'TEXT' : "OBJVERS = 'A'"}],
                       'RETRIEVEDATA' : ''},
            RSMDATASTATE_EXT= {'FIELDS' :['DTA', 'DTA_TYPE', 'RECORDS_ALL'],
                               'MAXROWS': self._max_rows,
                               'SELECTION' : [],
                               'RETRIEVEDATA' : ''},
            RSRREPDIR= {'FIELDS' :['COMPID', 'OBJVERS', 'OBJSTAT', 'INFOCUBE', 'COMPTYPE'],
                        'MAXROWS': self._max_rows,
                        'SELECTION' : [{'TEXT' : "OBJVERS = 'A'"}],
                        'RETRIEVEDATA' : ''},
            RSTRAN= {'FIELDS' :[],
                     'MAXROWS': self._max_rows,
                     'SELECTION' : [{'TEXT' : "OBJVERS = 'A'"}],
                     'RETRIEVEDATA' : ''},
            RSTRANT= {'FIELDS' :[],
                     'MAXROWS': self._max_rows,
                     'SELECTION' : [{'TEXT' : "OBJVERS = 'A' AND LANGU = 'E'"}],
                     'RETRIEVEDATA' : ''},
            RSUPDINFO= {'FIELDS' :[],
                        'MAXROWS': self._max_rows,
                        'SELECTION' : [{'TEXT' : "OBJVERS = 'A'"}],
                        'RETRIEVEDATA' : ''},
            RSZELTDIR= {'FIELDS' :[],
                        'MAXROWS': self._max_rows,
                        'SELECTION' : [{'TEXT' : "OBJVERS = 'A' AND DEFTP = 'REP'"}],
                        'RETRIEVEDATA' : ''},
            RSZELTTXT= {'FIELDS' :[],
                        'MAXROWS': self._max_rows,
                        'SELECTION' : [{'TEXT' : "OBJVERS = 'A' AND LANGU = 'E'"}],
                        'RETRIEVEDATA' : ''},
            RSSTATMANPART={'FIELDS' :['DTA', 'DTA_TYPE', 'DATUM_ANF', 'OLTPSOURCE', 'UPDMODE',
                                      'ANZ_RECS', 'INSERT_RECS', 'TIMESTAMP_ANF', 'SOURCE_DTA', 'SOURCE_DTA_TYPE'],
                           'MAXROWS': self._max_rows, # 0 brings all records back
                           'SELECTION' : [{'TEXT' : "(DTA_TYPE = 'CUBE' OR DTA_TYPE = 'ODSO') AND DATUM_ANF >= '20120101'"}], # eg. [{'TEXT' : "DTA = 'ZO00014'"}]
                           'RETRIEVEDATA' : ''}, #Blank means retrieve data
            USER_ADDRP= {'FIELDS' : [],
                        'MAXROWS' : self._max_rows,
                        'SELECTION' : [{'TEXT': "MANDT = '023'"}],
                        'RETRIEVEDATA' : ''}
        )

    def update_table_RFC (self,statusbar, progressBar, tableNumInt):

        sqlite_db_name = self._database
        SAP = SAP_table_to_sqlite_table()
        SAP.login_to_SAP()
        for tab in self.tab_spec.keys():
            print 'Reading SAP table ' + tab
            statusbar.showMessage('Reading SAP table ' + tab, 0)
            tableNumInt += 1
            progressBar.setValue(tableNumInt)
            #Retrieve data limited (set by self._max_rows) records at a time to conserve resources
            skip_rows = 0
            read_more = True
            first_time = True
            while read_more:
                print 'Retrieving ' + tab + ' records from SAP via RFC call'
                statusbar.showMessage('Retrieving ' + tab + ' records from SAP via RFC call', 0)
                RFC_READ_TABLE_output = SAP.read_SAP_table(tab, self.tab_spec[tab]['MAXROWS'], skip_rows, self.tab_spec[tab]['SELECTION'],
                                                           self.tab_spec[tab]['FIELDS'], self.tab_spec[tab]['RETRIEVEDATA'])
                retrieved_recs = len(RFC_READ_TABLE_output['DATA'])
                if first_time:
                    print RFC_READ_TABLE_output['OPTIONS']
                    i = 0
                    for v in RFC_READ_TABLE_output['FIELDS']:
                        i += 1
                        print v
                    print 'Number of entries : ' + repr(i)
                    statusbar.showMessage('Number of entries : ' + repr(i), 0)
                    QCoreApplication.processEvents()
                    append = False
                    first_time = False
                else:
                    append = True

                print 'Number of data records retrieved  from SAP : ' + repr(len(RFC_READ_TABLE_output['DATA']))
                statusbar.showMessage('Number of data records retrieved  from SAP : ' + repr(len(RFC_READ_TABLE_output['DATA'])),0)
                QCoreApplication.processEvents()

                if self.tab_spec[tab]['RETRIEVEDATA'] == '':
                    print 'Adding ' + tab + ' records ' + repr(skip_rows+ 1) + ' to ' + repr(skip_rows + retrieved_recs) + ' to sqlite database'
                    statusbar.showMessage('Adding ' + tab + ' records ' + repr(skip_rows+ 1) + ' to ' + repr(skip_rows + retrieved_recs) + ' to sqlite database',0)
                    QCoreApplication.processEvents()
                    SAP.update_sqlite_table(sqlite_db_name, tab, append, RFC_READ_TABLE_output)
                if not retrieved_recs == self._max_rows: #we got the last of the records from the SAP table
                    read_more = False
                else:
                    skip_rows = skip_rows + self._max_rows
                    read_more = True

        #Close SAP Connection
        SAP.close_SAP_connection()

    def create_flow_table(self):
        u"""
        This method creates the empty table that will ultimately contain all the database dataflows in form SOURCE ->
        TARGET
        :return: nothing
        """
        conn = sqlite3.connect(self._database)
        c = conn.cursor()
        #Delete table if already exists
        c.execute(u'DROP TABLE IF EXISTS ' + u'DATAFLOWS')
        c.execute(u'''CREATE TABLE DATAFLOWS (SOURCE text, TRANSFORM text, TARGET text, DERIVED_FROM text, SOURCE_TYPE text,
        SOURCE_SYSTEM text,SOURCE_SUB_TYP text, TARGET_TYPE text, TARGET_SUB_TYPE text, NAME text)
        ''')
        conn.commit()

    def update_flow_from_RSUPDINFO(self):
        u"""
        method create_flow_table must be called before this method to create table DATAFLOWS
        This method updates that table with relevant information from table RSUPDINFO concerning BW3.5 update rules
        :return: nothing
        """
        conn = sqlite3.connect(self._database)
        c = conn.cursor()
        #Assume target already exists and we are appending records
        c.executescript(u"""
        INSERT INTO DATAFLOWS
        (SOURCE, TRANSFORM, TARGET, DERIVED_FROM, SOURCE_TYPE, SOURCE_SYSTEM, SOURCE_SUB_TYP, TARGET_TYPE, TARGET_SUB_TYPE, NAME)
        SELECT DISTINCT CASE WHEN SUBSTR(A.ISOURCE,1,1)="8" THEN SUBSTR(A.ISOURCE,2,LENGTH(A.ISOURCE)) ELSE A.ISOURCE END,
        A.ISOURCE , A.INFOCUBE , "RSUPDINFO", "INFOSOURCE", "", "", "", B.LOGSYS, C.TXTLG
        FROM (RSUPDINFO AS A LEFT OUTER JOIN RSISOSMAP AS B ON A.ISOURCE = B.ISOURCE)
        LEFT OUTER JOIN RSIST AS C ON C.ISOURCE = A.ISOURCE
        WHERE A.OBJVERS = "A" AND A.OBJSTAT = "ACT" AND B.OBJVERS = "A" AND A.ISOURCE != A.INFOCUBE
        order by A.ISOURCE
        """)
        conn.commit()

    def update_flow_from_RSTRAN(self):
        u"""
        method create_flow_table must be called before this method to create table DATAFLOWS
        This method updates that table with relevant information from table RSTRAN concerning BI7 transformations
        :return: nothing
        """
        conn = sqlite3.connect(self._database)
        c = conn.cursor()
        result_set = []
        #Assume target already exists and we are appending records
        for row in c.execute (u'''SELECT A.SOURCENAME , A.TARGETNAME , "RSTRAN" , A.SOURCETYPE , A.SOURCENAME , A.SOURCESUBTYPE , A.TARGETTYPE ,
        A.TARGETSUBTYPE , B.TXTLG
        FROM RSTRAN AS A LEFT OUTER JOIN RSTRANT AS B ON A.TRANID = B.TRANID
        WHERE A.OBJVERS = "A" AND A.OBJSTAT = "ACT"
        '''):
            # Build a new line. We do this because SOURCENAME may have two parts separated by a blank character, we need
            #the first and second pieces.
            line = []
            i = len(row)
            for i in xrange(0,len(row)):
                col = row[i]
                if i == 0:
                    if col.find(u" ") == -1:  line.append(col)
                    else: line.append(col[0:col.find(u" ")])
                elif i == 4:
                    if col.find(u" ") == -1:  line.append(col)
                    else: line.append(col[col.rfind(u" ")+ 1 :len(col)])
                else: line.append(col)
            result_set.append(tuple(line))
            line = []

        for row in result_set:
            c.execute(u'''INSERT INTO DATAFLOWS (SOURCE, TARGET, DERIVED_FROM, SOURCE_TYPE, SOURCE_SYSTEM,
            SOURCE_SUB_TYP, TARGET_TYPE, TARGET_SUB_TYPE, NAME)
            VALUES (?,?,?,?,?,?,?,?,?)''', row)

        conn.commit()

    def update_flow_from_RSIOSMAP(self):
        u"""
        method create_flow_table must be called before this method to create table DATAFLOWS
        This method updates that table with relevant information from table RSIOSMAP with information concerning
        infosources
        :return: nothing
        """
        conn = sqlite3.connect(self._database)
        c = conn.cursor()
        #Assume target already exists and we are appending records
        c.executescript(u"""
        INSERT INTO DATAFLOWS
        (SOURCE, TARGET, DERIVED_FROM, SOURCE_TYPE, SOURCE_SYSTEM, SOURCE_SUB_TYP, TARGET_TYPE, TARGET_SUB_TYPE, NAME)
        SELECT A.LOGSYS,
        CASE WHEN SUBSTR(A.OLTPSOURCE,1,1)="8" THEN SUBSTR(A.OLTPSOURCE,2,LENGTH(A.OLTPSOURCE)) ELSE A.OLTPSOURCE END,
        "RSISOSMAP" , "EXTSYST TO DATASOURCE" , A.LOGSYS , "" , A.ISTYPE , A.ISTYPE , B.TXTLG
        FROM RSISOSMAP AS A LEFT OUTER JOIN RSIST AS B ON A.ISOURCE = B.ISOURCE
        WHERE A.OBJVERS = "A" AND B.OBJVERS = "A" AND A.OLTPSOURCE != A.ISOURCE
        """)
        conn.commit()
        c.executescript(u"""
        INSERT INTO DATAFLOWS
        (SOURCE, TARGET, DERIVED_FROM, SOURCE_TYPE, SOURCE_SYSTEM, SOURCE_SUB_TYP, TARGET_TYPE, TARGET_SUB_TYPE, NAME)
        SELECT CASE WHEN SUBSTR(A.OLTPSOURCE,1,1)="8" THEN SUBSTR(A.OLTPSOURCE,2,LENGTH(A.OLTPSOURCE)) ELSE A.OLTPSOURCE END,
        CASE WHEN SUBSTR(A.ISOURCE,1,1)="8" THEN SUBSTR(A.ISOURCE,2,LENGTH(A.ISOURCE)) ELSE A.ISOURCE END,
        "RSISOSMAP" , "DATASOURCE TO INFOSOURCE" , A.LOGSYS , "" , A.ISTYPE , A.ISTYPE , B.TXTLG
        FROM RSISOSMAP AS A LEFT OUTER JOIN RSIST AS B ON A.ISOURCE = B.ISOURCE
        WHERE A.OBJVERS = "A" AND B.OBJVERS = "A" AND A.OLTPSOURCE != A.ISOURCE
        """)
        conn.commit()

    def update_flow_from_RSBSPOKE(self):
        u"""
        method create_flow_table must be called before this method to create table DATAFLOWS
        This method updates that table with relevant information from table RSBSPOKE with information concerning BW3.5
        infospokes
        :return: nothing
        """
        conn = sqlite3.connect(self._database)
        c = conn.cursor()
        #Assume target already exists and we are appending records
        c.executescript(u"""
        INSERT INTO DATAFLOWS
        (SOURCE, TARGET, DERIVED_FROM, SOURCE_TYPE, SOURCE_SYSTEM, SOURCE_SUB_TYP, TARGET_TYPE, TARGET_SUB_TYPE, NAME)
        SELECT A.OHSOURCE , A.INFOSPOKE , "RSBSPOKE" , A.OHSRCTYPE , "P2WCLNT023" , "" , "INFSPOKE" ,
        A.OHDEST , B.TXTLG
        FROM RSBSPOKE AS A LEFT OUTER JOIN RSBSPOKET AS B ON A.INFOSPOKE = B.INFOSPOKE
        WHERE A.OBJVERS = "A" AND A.OBJSTAT = "ACT"
        """)
        conn.commit()

    def update_flow_from_RSBOHDEST(self):
        u"""
        method create_flow_table must be called before this method to create table DATAFLOWS
        This method updates that table with relevant information from table RSBOHDEST with information concerning BI7
        open hub destinations
        :return: nothing
        """
        conn = sqlite3.connect(self._database)
        c = conn.cursor()
        #Assume target already exists and we are appending records
        c.executescript(u"""
        INSERT INTO DATAFLOWS
        (SOURCE, TARGET, DERIVED_FROM, SOURCE_TYPE, SOURCE_SYSTEM, SOURCE_SUB_TYP, TARGET_TYPE, TARGET_SUB_TYPE, NAME)
        SELECT A.SOURCEOBJNM , A.OHDEST , "RSBOHDEST" , A.SOURCETLOGO , "P2WCLNT023" , A.SOURCETLOGOSUB ,
        "OPENHUB" ,
        A.OHDEST , B.TXTLG
        FROM RSBOHDEST AS A LEFT OUTER JOIN RSBOHDESTT AS B ON A.OHDEST = B.OHDEST
        WHERE A.OBJVERS = "A" AND A.SOURCETLOGO != "" AND A.OBJSTAT = "ACT"
        """)
        conn.commit()

    def update_flow_from_RSLDPSEL(self):
        u"""
        method create_flow_table must be called before this method to create table DATAFLOWS
        This method updates that table with relevant information from table RSLDPSEL and RSLDPIO to give information
        on flat file loads in infopackages.
        :return: nothing
        """
        conn = sqlite3.connect(self._database)
        c = conn.cursor()
        #Assume target already exists and we are appending records
        c.executescript(u"""
        INSERT INTO DATAFLOWS
        (SOURCE, TRANSFORM, TARGET, DERIVED_FROM, SOURCE_TYPE, SOURCE_SYSTEM, SOURCE_SUB_TYP, TARGET_TYPE, TARGET_SUB_TYPE, NAME)
        SELECT A.FILENAME, B.OLTPSOURCE, B.SOURCE, "RSLDPSEL", B.LOGSYS,  "", "",
        CASE WHEN B.OLTPTYP = "M" THEN "MD ATTRIB"
        WHEN B.OLTPTYP = "T" THEN "MD TEXTS"
        WHEN B.OLTPTYP = "H" THEN "MD HIER"
        ELSE B.OLTPTYP
        END,
        "", C.TEXT
        FROM (RSLDPSEL AS A LEFT OUTER JOIN RSLDPIO AS B ON A.LOGDPID = B.LOGDPID)
        LEFT OUTER JOIN RSLDPIOT AS C ON A.LOGDPID = C.LOGDPID
        WHERE A.OBJVERS = "A" AND B.OBJVERS = "A"
        --MASTER DATA
        AND (A.KIND = "T" or A.KIND = "M" OR A.KIND = "H")
        AND (B.OLTPTYP = "T" or B.OLTPTYP = "M" OR B.OLTPTYP = "H")
        AND A.FILENAME != ""
        """)
        conn.commit()

    def update_flow_from_RSDCUBEMULTI(self):
        u"""
        method create_flow_table must be called before this method to create table DATAFLOWS
        This method updates that table with relevant information from table RSDCUBEMULTI  concerning cube contents of
        multiproviders
        :return: nothing
        """
        conn = sqlite3.connect(self._database)
        c = conn.cursor()
        #Assume target already exists and we are appending records
        c.executescript(u"""
        INSERT INTO DATAFLOWS
        (SOURCE, TARGET, DERIVED_FROM, SOURCE_TYPE, SOURCE_SYSTEM, SOURCE_SUB_TYP, TARGET_TYPE, TARGET_SUB_TYPE, NAME)
        SELECT A.PARTCUBE , A.INFOCUBE , "RSDCUBEMULTI" , "CUBE" , "P2WCLNT023" , "" , "MULTIPROVIDER" ,
        "" , B.TXTLG
        FROM RSDCUBEMULTI AS A LEFT OUTER JOIN RSDCUBET AS B ON A.INFOCUBE = B.INFOCUBE
        WHERE A.OBJVERS = "A" AND B.OBJVERS = "A" And B.LANGU = "E"
        """)
        conn.commit()

    def update_flow_from_RSRREPDIR(self):
        u"""
        method create_flow_table must be called before this method to create table DATAFLOWS
        This method updates that table with relevant information from table RSRREPDIR to get information on report
        connections to cubes and multiproviders.
        :return: nothing
        """
        conn = sqlite3.connect(self._database)
        c = conn.cursor()
        #Assume target already exists and we are appending records
        c.executescript(u"""
        INSERT INTO DATAFLOWS
        (SOURCE, TARGET, DERIVED_FROM, SOURCE_TYPE, SOURCE_SYSTEM, SOURCE_SUB_TYP, TARGET_TYPE, TARGET_SUB_TYPE, NAME)
        SELECT A.INFOCUBE , A.COMPID , "RSRREPDIR" , "INFOCUBE" , "P2WCLNT023" , "" , A.COMPTYPE ,
        "" , C.TXTLG
        FROM (RSRREPDIR AS A LEFT OUTER JOIN RSZELTDIR AS B ON A.COMPID = B.MAPNAME)
        LEFT OUTER JOIN RSZELTTXT AS C on B.ELTUID = C.ELTUID
        WHERE A.OBJVERS = "A" AND A.OBJSTAT = "ACT" AND B.OBJVERS = "A" AND B.DEFTP = "REP"
        AND C.OBJVERS = "A" AND C.LANGU = "E"
        """)
        conn.commit()

    def create_mini_graph_2(self,start_node, forward, show_queries):
        u"""
        This method creates a graph in graphviz format for links in dataflow starting at node and going either forwards
        or backwards. Going forward from the node only nodes connected  in that direction are shown. For example if
        start_node is A and it connects A -> B -> C but als D connects to C as D -> C, D will NOT be shown.
        Similarly going backwards, if c -> B -> A but also C -> D and D does not go to A, then D will not be shown.
        :param start_node: Node to start the graph from
        :param forward: Boolean indicating if we are looking forward in the graph or backwards
        :param show_queries: Boolean. True if the map should show queries. False if not.
        :return: An sequenced list of lines which could be out put to a file and be read by graphviz "dot" program. Sequenced
                because start and end entry are required by graphviz program "dot"
        """
        conn = sqlite3.connect(self._database)
        c = conn.cursor()
        sql_fwd = u"SELECT DISTINCT TARGET FROM DATAFLOWS WHERE DERIVED_FROM != ? AND SOURCE =?"
        sql_bwd = u"SELECT DISTINCT SOURCE FROM DATAFLOWS WHERE DERIVED_FROM != ? AND TARGET =?"
        if show_queries: omit_derived_from_table = u""
        else: omit_derived_from_table = u"RSRREPDIR"
        complete = set()
        nodes = set()
        nodes.add(start_node)
        result_graph = []
        graph_set = set()
        while len(nodes) != 0:
            curr_node = nodes.pop()
            if forward:
                for row in c.execute(sql_fwd,(omit_derived_from_table, curr_node)):
                    if row[0] not in complete: nodes.add(row[0])
            else:
                for row in c.execute(sql_bwd,(omit_derived_from_table, curr_node)):
                    if row[0] not in complete: nodes.add(row[0])
            complete.add(curr_node)

        for el in complete:
            if forward:
                for row in c.execute(sql_fwd,(omit_derived_from_table, el)):
                    graph_set.add(u'"' + el + u'"' + u' -> ' + u'"' + row[0] + u'"' + u'\n')
            else:
                for row in c.execute(sql_bwd,(omit_derived_from_table, el)):
                    graph_set.add(u'"' + row[0] + u'"' + u' -> ' + u'"' + el + u'"' + u'\n')

        result_graph.append(u'digraph {ranksep=2\n')
        for line in graph_set: result_graph.append(line)
        result_graph.append(u'}')
        return result_graph

    def add_graphviz_iterables(self,list1, list2):
        list1.pop(0) #Remove first element u'digraph {ranksep=2\n'
        list1.pop()  # Remove last element u'}'
        list2.pop(0)
        list2.pop()
        list1.extend(list2) #Join the lists
        list1.insert(0,u'digraph {ranksep=2\n')
        list1.append(u'}')
        return list1

    def create_mini_graph_connections(self,start_node, show_queries):
        u"""
        This method creates a graph in graphviz format for links in dataflow starting at node and going BOTH forwards
        and backwards. Unlike methods create_mini_graph_bwd_fwd and create_mini_graph this method will capture all
        connectons. For example if start_node is A and it connects A -> B -> C but also D connects to C as D -> C, D WILL
        be shown together with its forward and backward dependencies. Similarly going backwards.
        :param start_node: Node to start the graph from
        :param show_queries: Boolean. True if the map should show queries. False if not.
        :return: An iterable of lines which could be out put to a file and be read by graphviz "dot" program
        """
        conn = sqlite3.connect(self._database)
        c = conn.cursor()
        sql_fwd = u"SELECT DISTINCT TARGET FROM DATAFLOWS WHERE DERIVED_FROM != ? AND SOURCE =?"
        sql_bwd = u"SELECT DISTINCT SOURCE FROM DATAFLOWS WHERE DERIVED_FROM != ? AND TARGET =?"
        if show_queries: omit_derived_from_table = u""
        else: omit_derived_from_table = u"RSRREPDIR"
        complete = set()
        nodes = set()
        nodes.add(start_node)
        result_graph = []
        graph_set = set()
        while len(nodes) != 0:
            curr_node = nodes.pop()
            for row in c.execute(sql_fwd,(omit_derived_from_table, curr_node)):
                if row[0] not in complete: nodes.add(row[0])
            for row in c.execute(sql_bwd,(omit_derived_from_table, curr_node)):
                if row[0] not in complete: nodes.add(row[0])
            complete.add(curr_node)

        for el in complete:
            for row in c.execute(sql_fwd,(omit_derived_from_table, el)):
                graph_set.add(u'"' + el + u'"' + u' -> ' + u'"' + row[0] + u'"' + u'\n')
            for row in c.execute(sql_bwd,(omit_derived_from_table, el)):
                graph_set.add(u'"' + row[0] + u'"' + u' -> ' + u'"' + el + u'"' + u'\n')

        result_graph.append(u'digraph {ranksep=2\n')
        for line in graph_set: result_graph.append(line)
        result_graph.append(u'}')
        return result_graph

    def create_full_graph(self):
        u"""
        Create a fully connected graph from all nodes in the DATAFLOWS table
        :return: An iterable of lines which could be out put to a file and be read by graphviz "dot" program
        """
        conn = sqlite3.connect(self._database)
        c = conn.cursor()
        out = set() #Removes duplicates
        out_graph = []
        out_graph.append(u'digraph {ranksep=2\n')
        for row in c.execute (u'''SELECT A.SOURCE, A.TARGET FROM DATAFLOWS AS A'''):
            out.add(u'"' + row[0] + u'"' + u' -> ' + u'"' + row[1] + u'"' + u'\n')
        for line in out: out_graph.append(line)
        out_graph.append(u'}')
        return out_graph

    def decorate_graph_sizes(self, graph_to_decorate):
        u"""
        Take input of a file ready to be graphed by dot and add information regarding number of records in the datastore
        """
        # Identify the nodes we are interested in - just the ones in the incoming graph
        nodes_we_want = []
        for line in graph_to_decorate:
            if u"->" in line: nodes_we_want.append(line.split(u'"')[1])

        conn = sqlite3.connect(self._database)
        c = conn.cursor()
        out_graph = []
        sizes = {}
        for node in nodes_we_want:
            row = c.execute(u"SELECT DTA, RECORDS_ALL FROM RSMDATASTATE_EXT WHERE DTA_TYPE != ? AND DTA = ?", (u"DTASRC", node))
            for r in row:
                sizes[r[0]] = int(r[1].replace(u',',u'')) # convert RECORDSALL in format "123,456,789" to integer

        #for row in c.execute('''SELECT DTA, RECORDSALL FROM RSMDATASTATE_EXT
        #WHERE DTATYPE != "DTASRC"
        #ORDER BY DTA
        #'''):
        #    sizes[row[0]] = int(row[1].replace(',','')) # convert RECORDSALL in format "123,456,789" to integer

        ####################################
        # Generate Size Ranges based on a regular distribution between lowest and highest
        ####################################
        try: max_size = max(sizes.values()) #Maximum number of records in any datastore
        except (ValueError): max_size = 0
        num_intervals = 100 #How finely we want the heat map to be divided
        size_ranges = []
        base_interval = max_size / num_intervals
        #Generate the intervals which will get a different color
        start = 1
        for i in range(num_intervals):
            size_ranges.append((int(i*base_interval),int((i+1)*base_interval))) # The intervals of size (tuple (start end))
        #Last interval should end at max_size, rounding errors can mean it dos not. Ensure it does
        last_range = (int((i-1)*base_interval), int(max_size+1))
        del size_ranges[-1]
        size_ranges.append(last_range)
        #Generate color values (in HSV format - http://www.graphviz.org/doc/info/attrs.html#k:color)
        #Hue, Saturation, Brightness. Hue in range 0.4 (green) to 0.0 (orange)
        HSV_tuples = [(-1.0*((x*1.0/num_intervals)-1)*0.4, 0.9, 0.9) for x in xrange(num_intervals)]
        #Loop over datastore records
        for data_store ,size in sizes.items():
            fmtString = u''
            i = 0
            for rg in size_ranges: #List of tuples of size intervals
                if size >= rg[0] and size < rg[1]:
                    fmtString = u'[color=white, fillcolor="' + repr(HSV_tuples[i]).lstrip(u'(').rstrip(u')') + u'", style="rounded,filled", shape=box]'
                    break
                i += 1
            if size == 0: fmtString = u'[color=red, style="rounded", shape=box]'
            out_graph.append(u'"' + data_store + u'" ' + fmtString + u'\n')
        del graph_to_decorate[-1] # remove delimiting "}"
        graph_to_decorate.extend(out_graph)
        graph_to_decorate.append(u'}') # Add back delimiter
        return graph_to_decorate

    def decorate_graph_BI7Flow(self, graph_to_decorate):
        u"""
        Take input of a file ready to be graphed by dot and add information regarding whether connections are BI7
        transformations or infosources
        """
        conn = sqlite3.connect(self._database)
        c = conn.cursor()
        #Get the source target connections in a form we can easily manipulate
        connections, out_graph, link = [], [], []
        for line in graph_to_decorate:
            try:
                link.append(line.split(u'"')[1])
                link.append (line.split(u'"')[3])
                row =  c.execute(u"SELECT DERIVED_FROM FROM DATAFLOWS WHERE SOURCE=? and TARGET=?",(str(link[0]),str(link[1])))
                for result in row: derived_from = result[0]
                if derived_from == u"RSUPDINFO" or derived_from == u"RSBSPOKE": out_graph.append(str(line).strip(u"\n") + u"[color=red]\n")
                elif derived_from == u"RSTRAN" or derived_from == u"RSBOHDEST": out_graph.append(str(line).strip(u"\n") + u"[style=bold,color=green]\n")
                else: out_graph.append(line)
                link = []
            except (IndexError):
                out_graph.append(line) #Header and trailer lines containing "{" and "}"

        return out_graph

    def decorate_graph_flow_volumes(self, graph_to_decorate):
        conn = sqlite3.connect(self._database)
        c = conn.cursor()
        connections, out_graph, link = [], [], []
        #This sql statement looks up records with a transaction date as near as possible to today
        rows = c.execute(u'''SELECT
                        CASE WHEN SUBSTR (OLTPSOURCE, 1, 1) = "8" THEN SUBSTR (OLTPSOURCE, 2, LENGTH(OLTPSOURCE))
                        ELSE OLTPSOURCE END,
                        DTA, ANZ_RECS, INSERT_RECS,
                        MAX(SUBSTR(DATUM_ANF,1,4)||"-"||SUBSTR(DATUM_ANF,5,2)||"-"||SUBSTR(DATUM_ANF,7,2))
                        FROM RSSTATMANPART
                        WHERE SUBSTR(DATUM_ANF,1,4)||"-"||SUBSTR(DATUM_ANF,5,2)||"-"||SUBSTR(DATUM_ANF,7,2) <= date('now')
                        GROUP BY OLTPSOURCE, DTA
                         ''')
        #Store OLTPSOURCE, DTA, ANZRECS, INSERTRECS in internal keyed structure
        OLTPSOURCE, DTA = dict(), dict()
        #Construct a dictionary like {OLTPSOURCE : {DTA : (ANZRECS, INSERTRECS),.....}, .....}
        for el in rows:
            try:
                DTA = OLTPSOURCE[el[0]] #Get the dict element to update it. Will fail with KeyError if not created previously
                DTA[el[1]] = el[2],el[3],el[4]
                OLTPSOURCE[el[0]] = DTA
            except (KeyError):
                OLTPSOURCE[el[0]] = {el[1]:(el[2],el[3],el[4])}

        for line in graph_to_decorate:
            try:
                link.append(line.split(u'"')[1])
                link.append (line.split(u'"')[3])
                try:
                    result = OLTPSOURCE[link[0]][link[1]] #Lookup one of the records stored before and reconstruct line if necessary
                    out_graph.append(line.strip(u"\n") + u"[label=" + u'"' + result[2] + u"\n" + result[0] + u"\n" + result[1] + u'"]' + u"\n")
                except (KeyError):
                    out_graph.append(line)
                link = []
            except (IndexError):
                out_graph.append(line) #Header and trailer lines containing "{" and "}"

        return out_graph

    def create_svg_file(self, graphviz_format_iterable, svg_file_out, dot_loc):
        #intermdiate text file
        graphviz_text_file = svg_file_out[0:svg_file_out.find(u'.')] + u'.txt'
        fo = open(graphviz_text_file,u'w')
        for line in graphviz_format_iterable:
            fo.write(unicode(str(line)))
        fo.close()
        # Call dot program to output svg file
        #dot_loc = "c:\\Program Files\\Graphviz2.38\\bin\\dot.exe"
        try:
            subprocess.call([dot_loc,u'-Tsvg', graphviz_text_file, u'-o',
                         svg_file_out], stderr = None, shell=False)
            subprocess.check_call([dot_loc,u'-Tsvg', graphviz_text_file, u'-o',
                         svg_file_out],stderr = None, shell=False)
        except (subprocess.CalledProcessError), e:
            print u"CalledProcessError error Handling......."
            print u"Returncode {0} command {1} output {2}".format(e.returncode, e.cmd, e.output)
        except OSError, e:
            print u"OSError error Handling......."
            print u"Returncode = {0} meaning '{1}' file = {2}".format(e.errno, e.strerror, e.filename)
        except ValueError, e:
            print u"ValueError error Handling......."

        return

    def get_nodes(self):
        u"""
            get the graph nodes in an ordered list if they exist
        """
        conn = sqlite3.connect(self._database)
        c = conn.cursor()
        nodes = set()
        for row in c.execute(u'''SELECT SOURCE, TARGET FROM DATAFLOWS
        '''):
            if row[0] != u'': nodes.add(row[0])
            if row[1] != u'': nodes.add(row[1])
        return sorted(list(nodes))

    def get_node(self,node):
        u"""
        Check if a given node exists
        :returns boolean False if node does not exist and True if exists
        """
        conn = sqlite3.connect(self._database)
        c = conn.cursor()
        #note that in PyQt4 incoming node is QString and sqlite3 does not like that
        c.execute(u"SELECT SOURCE FROM DATAFLOWS WHERE SOURCE =? OR TARGET=?",(str(node),str(node)))
        row = c.fetchone()
        if row is None: return False
        else: return True

    def update_text_table(self):
        """
        Populate an internal table of BW object texts for easy retrieval at various points. Need to have loaded
        base SAP tables via RFC first.
        :return: none
        """
        conn = sqlite3.connect(self._database)
        c = conn.cursor()
        #Delete table if already exists
        c.execute(u'DROP TABLE IF EXISTS ' + u'TEXTS')
        c.execute(u'''CREATE TABLE TEXTS (OBJECT text, TEXT text, SOURCE text)''')
        data = []
        #Infocube
        for row in c.execute("SELECT INFOCUBE, TXTLG FROM RSDCUBET WHERE OBJVERS ='A'"):
            data.append((row[0],row[1],'RSDCUBET'))
        #DSO
        for row in c.execute("SELECT ODSOBJECT, TXTLG FROM RSDODSOT WHERE OBJVERS ='A'"):
            data.append((row[0],row[1],'RSDODSOT'))
        #Infospoke
        for row in c.execute("SELECT INFOSPOKE, TXTLG FROM RSBSPOKET WHERE OBJVERS ='A'"):
            data.append((row[0],row[1],'RSBSPOKET'))
        #Open Hub
        for row in c.execute("SELECT OHDEST, TXTLG FROM RSBOHDESTT WHERE OBJVERS ='A'"):
            data.append((row[0],row[1],'RSBOHDESTT'))
        #Query
        for row in c.execute("SELECT A.MAPNAME, B.TXTLG FROM RSZELTDIR AS A INNER JOIN RSZELTTXT AS B ON A.ELTUID = B.ELTUID \
                     WHERE A.OBJVERS='A' AND B.LANGU='E'"):
            data.append((row[0],row[1],'RSZELTTXT'))
        c.executemany("INSERT INTO TEXTS (OBJECT, TEXT, SOURCE) VALUES (?,?,?)",data)
        conn.commit()

    def get_node_text(self,node):
        conn = sqlite3.connect(self._database)
        c = conn.cursor()
        c.execute(u"SELECT TEXT FROM TEXTS WHERE OBJECT=?", (node,))
        row = c.fetchone()
        if row is not None: return row[0]
        else: return u""

    def create_user_activity_table(self):
        """
        Creaet a table "user_activity" containing a LISTCUBE from 0TCT_CA1
        :return: Nothing
        """
        LC = SAP_LISTCUBE_to_sqlite_table()
        LC.login_to_SAP()
        infoprovider = '0TCT_CA1'
        chars_req = ['0TCTIFPROV','0TCTBISBOBJ','0TCTBIOTYPE','0TCTBISOTYP','0TCTIFTYPE','0TCTUSERNM','0CALDAY']
        #Since multiple months is too much data for SAP RFC to handle, break data pull into month buckets. First month
        #is added fresh to sqlite table the other months are appended
        high_dt = datetime.date.today()
        low_dt = datetime.date.today() - datetime.timedelta(days=+31)
        append = False
        for i in range(3):
            high = repr(high_dt.year) + repr(high_dt.month).zfill(2) + repr(high_dt.day).zfill(2) #yyyymmdd
            low = repr(low_dt.year) + repr(low_dt.month).zfill(2) + repr(low_dt.day).zfill(2) #yyyymmdd
            print high, low
            kfs_req = ['0TCTQUCOUNT', '0TCTTIMEALL']
            restrictions = [{'CHANM' : '0TCTBIOTYPE', 'SIGN' : 'I', 'COMPOP' : 'EQ', 'LOW' : 'XLWB'},
                        {'CHANM' : '0TCTBIOTYPE', 'SIGN' : 'I', 'COMPOP' : 'EQ', 'LOW' : 'TMPL'},
                        {'CHANM' : '0TCTBISOTYP', 'SIGN' : 'I', 'COMPOP' : 'EQ', 'LOW' : 'ELEM'},
                        {'CHANM' : '0CALDAY', 'SIGN' : 'I', 'COMPOP' : 'BT', 'LOW' : low, 'HIGH' : high}]
            result = LC.read_infocube(infoprovider, chars_req, kfs_req, restrictions)
            LC.write_sqlite(result, self._database, 'USER_ACTIVITY', chars_req+kfs_req, append)
            append = True
            high_dt = low_dt - datetime.timedelta(days=+1)
            low_dt = high_dt - datetime.timedelta(days=+31)

    #TODO Fix RSBSPOKE assignment of TARGET_TYPE (should be "INFOSPOKE, seems to be OHSOURCE)
    #TODO Fix the progress bar. When have long running table loads it does not update.
    #TODO reduce the ime to download RSSTATMANPART table from SAP and, more importantly, to load it to sqlite db


