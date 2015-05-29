import subprocess
import os
import os.path

__author__ = 'U104675'
import sys
import sqlite3
#sys.path.append('C:\\Users\\u104675\\Jon_Waterhouse_Docs\\OneDrive - Eastman Koda~1\\PythonProjects\\')
sys.path.append(os.path.dirname(os.getcwd()))
import LoadToSqlite as SQL
class BWFlowTable():
    """
    This class is used to create a database of SAP tables, currently from SAP text downloads using SE16.

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
    """
    def __init__(self, database):
        """
        :param database: sets the database name storing all tje internal information
        :return: nothing
        """
        self._database = database

    def update_table(self,table_file_map, unwanted_cols):
        """
        Populates specified sqlite databse tables from flat files in SAP "Unconverted" output format ie fields
        separated by "|"
        :param database: database we will create tables in
        :param table_file_map: dictionary with key target table name to be created and value the source text file
        :param unwanted_cols: Ability to remove columns in incoming text file. Specified as a list with column numbers
        which need not be in correct sequence
        :return: nothing
        """
        for table,file in table_file_map.items():
            SQL.TextInSQL(self._database, file, table, unwanted_cols)

    def create_flow_table(self):
        """
        This method creates the empty table that will ultimately contain all the database dataflows in form SOURCE ->
        TARGET
        :return: nothing
        """
        conn = sqlite3.connect(self._database)
        c = conn.cursor()
        #Delete table if already exists
        c.execute('DROP TABLE IF EXISTS ' + 'DATAFLOWS')
        c.execute('''CREATE TABLE DATAFLOWS (SOURCE text, TRANSFORM text, TARGET text, DERIVED_FROM text, SOURCE_TYPE text,
        SOURCE_SYSTEM text,SOURCE_SUB_TYP text, TARGET_TYPE text, TARGET_SUB_TYPE text, NAME text)
        ''')
        conn.commit()

    def update_flow_from_RSUPDINFO(self):
        """
        method create_flow_table must be called before this method to create table DATAFLOWS
        This method updates that table with relevant information from table RSUPDINFO concerning BW3.5 update rules
        :return: nothing
        """
        conn = sqlite3.connect(self._database)
        c = conn.cursor()
        #Assume target already exists and we are appending records
        c.executescript("""
        INSERT INTO DATAFLOWS
        (SOURCE, TRANSFORM, TARGET, DERIVED_FROM, SOURCE_TYPE, SOURCE_SYSTEM, SOURCE_SUB_TYP, TARGET_TYPE, TARGET_SUB_TYPE, NAME)
        SELECT DISTINCT CASE WHEN SUBSTR(A.ISOURCE,1,1)="8" THEN SUBSTR(A.ISOURCE,2,LENGTH(A.ISOURCE)) ELSE A.ISOURCE END,
        A.ISOURCE , A.INFOCUBE , "RSUPDINFO", "INFOSOURCE", "", "", "", B.LOGSYS, C.TXTLG
        FROM (RSUPDINFO AS A LEFT OUTER JOIN RSISOSMAP AS B ON A.ISOURCE = B.ISOURCE)
        LEFT OUTER JOIN RSIS AS C ON C.ISOURCE = A.ISOURCE
        WHERE A.OBJVERS = "A" AND A.OBJSTAT = "ACT" AND B.OBJVERS = "A" AND A.ISOURCE != A.INFOCUBE
        order by A.ISOURCE
        """)
        conn.commit()

    def update_flow_from_RSTRAN(self):
        """
        method create_flow_table must be called before this method to create table DATAFLOWS
        This method updates that table with relevant information from table RSTRAN concerning BI7 transformations
        :return: nothing
        """
        conn = sqlite3.connect(self._database)
        c = conn.cursor()
        result_set = []
        #Assume target already exists and we are appending records
        for row in c.execute ('''SELECT A.SOURCENAME , A.TARGETNAME , "RSTRAN" , A.SOURCETYPE , A.SOURCENAME , A.SOURCESUBTYPE , A.TARGETTYPE ,
        A.TARGETSUBTYPE , A.TXTLG FROM RSTRAN AS A WHERE A.OBJVERS = "A" AND A.OBJSTAT = "ACT"
        '''):
            # Build a new line. We do this because SOURCENAME may have two parts separated by a blank character, we need
            #the first and second pieces.
            line = []
            i = len(row)
            for i in range(0,len(row)):
                col = row[i]
                if i == 0:
                    if col.find(" ") == -1:  line.append(col)
                    else: line.append(col[0:col.find(" ")])
                elif i == 4:
                    if col.find(" ") == -1:  line.append(col)
                    else: line.append(col[col.rfind(" ")+ 1 :len(col)])
                else: line.append(col)
            result_set.append(tuple(line))
            line = []

        for row in result_set:
            c.execute('''INSERT INTO DATAFLOWS (SOURCE, TARGET, DERIVED_FROM, SOURCE_TYPE, SOURCE_SYSTEM,
            SOURCE_SUB_TYP, TARGET_TYPE, TARGET_SUB_TYPE, NAME)
            VALUES (?,?,?,?,?,?,?,?,?)''', row)

        conn.commit()

    def update_flow_from_RSIOSMAP(self):
        """
        method create_flow_table must be called before this method to create table DATAFLOWS
        This method updates that table with relevant information from table RSIOSMAP with information concerning
        infosources
        :return: nothing
        """
        conn = sqlite3.connect(self._database)
        c = conn.cursor()
        #Assume target already exists and we are appending records
        c.executescript("""
        INSERT INTO DATAFLOWS
        (SOURCE, TARGET, DERIVED_FROM, SOURCE_TYPE, SOURCE_SYSTEM, SOURCE_SUB_TYP, TARGET_TYPE, TARGET_SUB_TYPE, NAME)
        SELECT A.LOGSYS,
        CASE WHEN SUBSTR(A.OLTPSOURCE,1,1)="8" THEN SUBSTR(A.OLTPSOURCE,2,LENGTH(A.OLTPSOURCE)) ELSE A.OLTPSOURCE END,
        "RSISOSMAP" , "EXTSYST TO DATASOURCE" , A.LOGSYS , "" , A.ISTYPE , A.ISTYPE , B.TXTLG
        FROM RSISOSMAP AS A LEFT OUTER JOIN RSIS AS B ON A.ISOURCE = B.ISOURCE
        WHERE A.OBJVERS = "A" AND B.OBJVERS = "A" AND A.OLTPSOURCE != A.ISOURCE
        """)
        conn.commit()
        c.executescript("""
        INSERT INTO DATAFLOWS
        (SOURCE, TARGET, DERIVED_FROM, SOURCE_TYPE, SOURCE_SYSTEM, SOURCE_SUB_TYP, TARGET_TYPE, TARGET_SUB_TYPE, NAME)
        SELECT CASE WHEN SUBSTR(A.OLTPSOURCE,1,1)="8" THEN SUBSTR(A.OLTPSOURCE,2,LENGTH(A.OLTPSOURCE)) ELSE A.OLTPSOURCE END,
        CASE WHEN SUBSTR(A.ISOURCE,1,1)="8" THEN SUBSTR(A.ISOURCE,2,LENGTH(A.ISOURCE)) ELSE A.ISOURCE END,
        "RSISOSMAP" , "DATASOURCE TO INFOSOURCE" , A.LOGSYS , "" , A.ISTYPE , A.ISTYPE , B.TXTLG
        FROM RSISOSMAP AS A LEFT OUTER JOIN RSIS AS B ON A.ISOURCE = B.ISOURCE
        WHERE A.OBJVERS = "A" AND B.OBJVERS = "A" AND A.OLTPSOURCE != A.ISOURCE
        """)
        conn.commit()

    def update_flow_from_RSBSPOKE(self):
        """
        method create_flow_table must be called before this method to create table DATAFLOWS
        This method updates that table with relevant information from table RSBSPOKE with information concerning BW3.5
        infospokes
        :return: nothing
        """
        conn = sqlite3.connect(self._database)
        c = conn.cursor()
        #Assume target already exists and we are appending records
        c.executescript("""
        INSERT INTO DATAFLOWS
        (SOURCE, TARGET, DERIVED_FROM, SOURCE_TYPE, SOURCE_SYSTEM, SOURCE_SUB_TYP, TARGET_TYPE, TARGET_SUB_TYPE, NAME)
        SELECT A.OHSOURCE , A.INFOSPOKE , "RSBSPOKE" , A.OHSRCTYPE , "P2WCLNT023" , "" , "INFOSPOKE" ,
        A.OHDEST , A.TXTLG
        FROM RSBSPOKE AS A
        WHERE A.OBJVERS = "A" AND A.OBJSTAT = "ACT"
        """)
        conn.commit()

    def update_flow_from_RSBOHDEST(self):
        """
        method create_flow_table must be called before this method to create table DATAFLOWS
        This method updates that table with relevant information from table RSBOHDEST with information concerning BI7
        open hub destinations
        :return: nothing
        """
        conn = sqlite3.connect(self._database)
        c = conn.cursor()
        #Assume target already exists and we are appending records
        c.executescript("""
        INSERT INTO DATAFLOWS
        (SOURCE, TARGET, DERIVED_FROM, SOURCE_TYPE, SOURCE_SYSTEM, SOURCE_SUB_TYP, TARGET_TYPE, TARGET_SUB_TYPE, NAME)
        SELECT A.SOURCEOBJNM , A.OHDEST , "RSBOHDEST" , A.SOURCETLOGO , "P2WCLNT023" , A.SOURCETLOGOSUB ,
        "OPENHUB" ,
        A.OHDEST , A.TXTLG
        FROM RSBOHDEST AS A
        WHERE A.OBJVERS = "A" AND A.SOURCETLOGO != "" AND A.OBJSTAT = "ACT"
        """)
        conn.commit()

    def update_flow_from_RSLDPSEL(self):
        """
        method create_flow_table must be called before this method to create table DATAFLOWS
        This method updates that table with relevant information from table RSLDPSEL and RSLDPIO to give information
        on flat file loads in infopackages.
        :return: nothing
        """
        conn = sqlite3.connect(self._database)
        c = conn.cursor()
        #Assume target already exists and we are appending records
        c.executescript("""
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
        """
        method create_flow_table must be called before this method to create table DATAFLOWS
        This method updates that table with relevant information from table RSDCUBEMULTI  concerning cube contents of
        multiproviders
        :return: nothing
        """
        conn = sqlite3.connect(self._database)
        c = conn.cursor()
        #Assume target already exists and we are appending records
        c.executescript("""
        INSERT INTO DATAFLOWS
        (SOURCE, TARGET, DERIVED_FROM, SOURCE_TYPE, SOURCE_SYSTEM, SOURCE_SUB_TYP, TARGET_TYPE, TARGET_SUB_TYPE, NAME)
        SELECT A.PARTCUBE , A.INFOCUBE , "RSDCUBEMULTI" , "CUBE" , "P2WCLNT023" , "" , "MULTIPROVIDER" ,
        "" , B.TXTLG
        FROM RSDCUBEMULTI AS A LEFT OUTER JOIN RSDCUBET AS B ON A.INFOCUBE = B.INFOCUBE
        WHERE A.OBJVERS = "A" AND B.OBJVERS = "A" And B.LANGU = "E"
        """)
        conn.commit()

    def update_flow_from_RSRREPDIR(self):
        """
        method create_flow_table must be called before this method to create table DATAFLOWS
        This method updates that table with relevant information from table RSRREPDIR to get information on report
        connections to cubes and multiproviders.
        :return: nothing
        """
        conn = sqlite3.connect(self._database)
        c = conn.cursor()
        #Assume target already exists and we are appending records
        c.executescript("""
        INSERT INTO DATAFLOWS
        (SOURCE, TARGET, DERIVED_FROM, SOURCE_TYPE, SOURCE_SYSTEM, SOURCE_SUB_TYP, TARGET_TYPE, TARGET_SUB_TYPE, NAME)
        SELECT A.INFOCUBE , A.COMPID , "RSRREPDIR" , "INFOCUBE" , "P2WCLNT023" , "" , A.COMPTYPE ,
        "" , B.TXTLG
        FROM RSRREPDIR AS A LEFT OUTER JOIN RSZELTDIR AS B ON A.COMPID = B.MAPNAME
        WHERE A.OBJVERS = "A" AND A.OBJSTAT = "ACT" AND B.OBJVERS = "A" AND B.DEFTP = "REP"
        """)
        conn.commit()

    def create_mini_graph(self, start_node, forward, show_queries):
        """
        This method creates a graph in graphviz format for links in dataflow starting at node and going EITHER forwards
        (forward = True) or backwards (forwards = False). Going forward from the node only nodes connected  in that
        direction are shown. For example if start_node is A and it connects A -> B -> C but als D connects to C as
        D -> C, D will NOT be shown. Similarly going backwards, if c -> B -> A but also C -> D and D does not go to A,
        then D will not be shown.
        :param start_node: Node to start the graph from
        :param forward: Direction to explore the data flow
        :return: An iterable lines which could be out put to a file and be read by graphviz "dot" program
        """
        result_graph = self._create_mini_graph_recursion(start_node,forward, [], show_queries)
        out_graph = []
        out_graph.append('digraph {ranksep=2\n')
        for line in set(result_graph): out_graph.append(line)
        out_graph.append('}')
        return out_graph

    def create_mini_graph_bwd_fwd(self, start_node, show_queries):
        """
        This method creates a graph in graphviz format for links in dataflow starting at node and going BOTH forwards
        and backwards. Going forward from the node only nodes connected  in that direction are shown. For example if
        start_node is A and it connects A -> B -> C but als D connects to C as D -> C, D will NOT be shown.
        Similarly going backwards, if c -> B -> A but also C -> D and D does not go to A, then D will not be shown.
        :param start_node: Node to start the graph from
        :return: An iterable of lines which could be out put to a file and be read by graphviz "dot" program
        """
        result_graph_fwd = self._create_mini_graph_recursion(start_node,True, [], show_queries)
        result_graph_bwd = self._create_mini_graph_recursion(start_node,False, [], show_queries)
        out_graph = []
        out = set()
        out_graph.append('digraph {ranksep=2\n')
        #Remove duplicates by means of set
        for line in result_graph_fwd: out.add(line)
        for line in result_graph_bwd: out.add(line)
        for line in out: out_graph.append(line)
        out_graph.append('}')
        return out_graph

    def create_mini_graph_connections(self,start_node, show_queries):
        """
        This method creates a graph in graphviz format for links in dataflow starting at node and going BOTH forwards
        and backwards. Unlike methods create_mini_graph_bwd_fwd and create_mini_graph this method will capture all
        connectons. For example if start_node is A and it connects A -> B -> C but also D connects to C as D -> C, D WILL
        be shown together with its forward and backward dependencies. Similarly going backwards.
        :param start_node: Node to start the graph from
        :return: An iterable of lines which could be out put to a file and be read by graphviz "dot" program
        """
        conn = sqlite3.connect(self._database)
        c = conn.cursor()
        sql_fwd = "SELECT DISTINCT TARGET FROM DATAFLOWS WHERE DERIVED_FROM != ? AND SOURCE =?"
        sql_bwd = "SELECT DISTINCT SOURCE FROM DATAFLOWS WHERE DERIVED_FROM != ? AND TARGET =?"
        if show_queries: omit_derived_from_table = ""
        else: omit_derived_from_table = "RSRREPDIR"
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
                graph_set.add('"' + el + '"' + ' -> ' + '"' + row[0] + '"' + '\n')
            for row in c.execute(sql_bwd,(omit_derived_from_table, el)):
                graph_set.add('"' + row[0] + '"' + ' -> ' + '"' + el + '"' + '\n')

        result_graph.append('digraph {ranksep=2\n')
        for line in graph_set: result_graph.append(line)
        result_graph.append('}')
        return result_graph

    def _create_mini_graph_recursion(self, start_node, forward, result_graph, show_queries):
        """
        A convenience method to handle recirsion in the look up of all related nodes in the graph
        :param start_node: current start node
        :param forward: which direction we are looking up forward = True or False
        :param result_graph:
        :return: An iterable of lines which could be out put to a file and be read by graphviz "dot" program
        """
        conn = sqlite3.connect(self._database)
        c = conn.cursor()
        sql_fwd = "SELECT DISTINCT TARGET FROM DATAFLOWS WHERE DERIVED_FROM != ? AND SOURCE =?"
        sql_bwd = "SELECT DISTINCT SOURCE FROM DATAFLOWS WHERE DERIVED_FROM != ? AND TARGET =?"
        if forward: sql = sql_fwd
        else: sql = sql_bwd
        if show_queries: omit_derived_from_table = ""
        else: omit_derived_from_table = "RSRREPDIR"
        for row in c.execute(sql, (omit_derived_from_table, start_node,)):
            if forward: graph_link = '"' + start_node + '"' + ' -> ' + '"' + row[0] + '"' + '\n'
            else: graph_link = '"' + row[0] + '"' + ' -> ' + '"' + start_node + '"' + '\n'
            result_graph.append(graph_link)
            if row[0] != start_node: self._create_mini_graph_recursion(row[0],forward, result_graph, show_queries)
        return result_graph

    def create_full_graph(self):
        """
        Create a fully connected graph from all nodes in the DATAFLOWS table
        :return: An iterable of lines which could be out put to a file and be read by graphviz "dot" program
        """
        conn = sqlite3.connect(self._database)
        c = conn.cursor()
        out = set() #Removes duplicates
        out_graph = []
        out_graph.append('digraph {ranksep=2\n')
        for row in c.execute ('''SELECT A.SOURCE, A.TARGET FROM DATAFLOWS AS A'''):
            out.add('"' + row[0] + '"' + ' -> ' + '"' + row[1] + '"' + '\n')
        for line in out: out_graph.append(line)
        out_graph.append('}')
        return out_graph

    def decorate_graph_sizes(self, graph_to_decorate):
        """
        Take input of a file ready to be graphed by dot and add information regarding number of records in the datastore
        """
        # Identify the nodes we are interested in - just the ones in the incoming graph
        nodes_we_want = []
        for line in graph_to_decorate:
            if "->" in line: nodes_we_want.append(line.split('"')[1])

        conn = sqlite3.connect(self._database)
        c = conn.cursor()
        out_graph = []
        sizes = {}
        for node in nodes_we_want:
            row = c.execute("SELECT DTA, RECORDSALL FROM RSMDATASTATE_EXT WHERE DTATYPE != ? AND DTA = ?", ("DATASRC", node))
            for r in row:
                sizes[r[0]] = int(r[1].replace(',','')) # convert RECORDSALL in format "123,456,789" to integer

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
        base_interval = int(max_size / num_intervals)
        #Generate the intervals which will get a different color
        start = 1
        for i in range(num_intervals):
            size_ranges.append(range(i*base_interval,(i+1)*base_interval)) # The intervals of size
        #Last interval should end at max_size, rounding errors can mean it dos not. Ensure it does
        last_range = range((i-1)*base_interval, max_size+1)
        del size_ranges[-1]
        size_ranges.append(last_range)
        #Generate color values (in HSV format - http://www.graphviz.org/doc/info/attrs.html#k:color)
        #Hue, Saturation, Brightness. Hue in range 0.4 (green) to 0.0 (orange)
        HSV_tuples = [(-1.0*((x*1.0/num_intervals)-1)*0.4, 0.9, 0.9) for x in range(num_intervals)]
        #Loop over datastore records
        for data_store ,size in sizes.items():
            fmtString = ''
            i = 0
            for rg in size_ranges:
                if size in rg:
                    fmtString = '[color=white, fillcolor="' + repr(HSV_tuples[i]).lstrip('(').rstrip(')') + '", style="rounded,filled", shape=box]'
                    break
                i += 1
            if size == 0: fmtString = '[color=red, style="rounded", shape=box]'
            out_graph.append('"' + data_store + '" ' + fmtString + '\n')
        del graph_to_decorate[-1] # remove delimiting "}"
        graph_to_decorate.extend(out_graph)
        graph_to_decorate.append('}') # Add back delimiter
        return graph_to_decorate

    def decorate_graph_BI7Flow(self, graph_to_decorate):
        """
        Take input of a file ready to be graphed by dot and add information regarding whether connections are BI7
        transformations or infosources
        """
        conn = sqlite3.connect(self._database)
        c = conn.cursor()
        #Get the source target connections in a form we can easily manipulate
        connections, out_graph, link = [], [], []
        for line in graph_to_decorate:
            try:
                link.append(line.split('"')[1])
                link.append (line.split('"')[3])
                row =  c.execute("SELECT DERIVED_FROM FROM DATAFLOWS WHERE SOURCE=? and TARGET=?",(link[0],link[1]))
                for result in row: derived_from = result[0]
                if derived_from == "RSUPDINFO" or derived_from == "RSBSPOKE": out_graph.append(line.strip("\n") + "[color=red]\n")
                elif derived_from == "RSTRAN" or derived_from == "RSBOHDEST": out_graph.append(line.strip("\n") + "[style=bold,color=green]\n")
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
        rows = c.execute('''SELECT
                        CASE WHEN SUBSTR (OLTPSOURCE, 1, 1) = "8" THEN SUBSTR (OLTPSOURCE, 2, LENGTH(OLTPSOURCE))
                        ELSE OLTPSOURCE END,
                        DTA, ANZRECS, INSERTRECS,
                        MAX(SUBSTR(DATUMANF,7,4)||"-"||SUBSTR(DATUMANF,4,2)||"-"||SUBSTR(DATUMANF,1,2))
                        FROM RSSTATMANPART
                        WHERE SUBSTR(DATUMANF,7,4)||"-"||SUBSTR(DATUMANF,4,2)||"-"||SUBSTR(DATUMANF,1,2) < date('now')
                        GROUP BY OLTPSOURCE, DTA
                         ''')
        #Store OLTPSOURCE, DTA, ANZRECS, INSERTRECS in internal keyed structure
        OLTPSOURCE, DTA = dict(), dict()
        #Construct a dictionary like {OLTPSOURCE : {DTA : (ANZRECS, INSERTRECS),.....}, .....}
        for el in rows:
            try:
                DTA = OLTPSOURCE[el[0]] #Get the dict element to update it. Will fail with KeyError if not created previously
                DTA[el[1]] = el[2],el[3]
                OLTPSOURCE[el[0]] = DTA
            except (KeyError):
                OLTPSOURCE[el[0]] = {el[1]:(el[2],el[3])}

        for line in graph_to_decorate:
            try:
                link.append(line.split('"')[1])
                link.append (line.split('"')[3])
                try:
                    result = OLTPSOURCE[link[0]][link[1]] #Lookup one of the records stored before and reconstruct line if necessary
                    out_graph.append(line.strip("\n") + "[label=" + '"' + result[0] + "\n" + result[1] + '"]' + "\n")
                except (KeyError):
                    out_graph.append(line)
                link = []
            except (IndexError):
                out_graph.append(line) #Header and trailer lines containing "{" and "}"

        return out_graph

    def create_svg_file(self, graphviz_format_iterable, svg_file_out, dot_loc):
        #intermdiate text file
        graphviz_text_file = svg_file_out[0:svg_file_out.find('.')] + '.txt'
        fo = open(graphviz_text_file,'w')
        for line in graphviz_format_iterable:
            fo.write(line)
        fo.close()
        # Call dot program to output svg file
        #dot_loc = "c:\\Program Files\\Graphviz2.38\\bin\\dot.exe"
        try:
            subprocess.call([dot_loc,'-Tsvg', graphviz_text_file, '-o',
                         svg_file_out], stderr = None, shell=False)
            subprocess.check_call([dot_loc,'-Tsvg', graphviz_text_file, '-o',
                         svg_file_out],stderr = None, shell=False)
        except (subprocess.CalledProcessError) as e:
            print ("CalledProcessError error Handling.......")
            print("Returncode {0} command {1} output {2}".format(e.returncode, e.cmd, e.output))
        except OSError as e:
            print ("OSError error Handling.......")
            print("Returncode = {0} meaning '{1}' file = {2}".format(e.errno, e.strerror, e.filename))
        except ValueError as e:
            print ("ValueError error Handling.......")

        return

    def get_nodes(self):
        """
            get the graph nodes in an ordered list if they exist
        """
        conn = sqlite3.connect(self._database)
        c = conn.cursor()
        nodes = set()
        for row in c.execute('''SELECT SOURCE, TARGET FROM DATAFLOWS
        '''):
            if row[0] != '': nodes.add(row[0])
            if row[1] != '': nodes.add(row[1])
        return sorted(list(nodes))

    def get_node_text(self,node):
        conn = sqlite3.connect(self._database)
        c = conn.cursor()
        c.execute("SELECT TXTLG FROM RSDCUBET WHERE INFOCUBE=? AND OBJVERS=?", (node,'A'))
        row = c.fetchone()
        if row is not None: return row[0]
        else:
            c.execute("SELECT TXTLG FROM RSDODSOT WHERE ODSOBJECT=? AND OBJVERS=?", (node,'A'))
            row = c.fetchone()
            if row is not None: return row[0]
            else: return ""

    def _unsplit(self, fileIn, fileOut, field_sep):
        """
        takes a file downloaded by ZSE16 in unconverted format, where the lines
        are long enough to have wrapped. Joins the two have lines
        """
        fi = open(fileIn,'r')
        fo = open(fileOut,'w')
        inCount = 0
        outCount = 0
        l1 = []
        l2 = []
        for line in fi:
            if line[0] != "|": continue
            if inCount%2 == 0:
                ln = line.rstrip(field_sep + '\n')
                l1 = ln.split(field_sep)
            else:
                ln = line.rstrip('\n')
                l2 = ln.split(field_sep)
                l1.extend(l2)
                fo.write(field_sep.join(l1) + '|\n')
                l1, l2 = [], []
                outCount += 1
            inCount += 1
        fi.close()
        fo.close()

if __name__ == "__main__":
    #path = 'C:\\Users\\u104675\\OneDrive - Eastman Kodak Company\\P2238 Reorganisation\\MDGeogAttribs\\'
    """
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
    """
    path = ''
    database = path + 'BWStructure.db'
    unwanted_cols = []
    t = BWFlowTable(database)
    fin = "RSLDPSEL.txt"
    fout = "RSLDPSELUnsplit.txt"
    sep = "|"
    t._unsplit(fin, fout, sep)
    table_file_map = {"RSTRAN": "RSTRAN.txt", 'RSISOSMAP':"RSISOSMAP.txt",'RSLDPSEL':fout,
                      'RSLDPIO':"RSLDPIO.txt", 'RSLDPIOT': "RSLDPIOT.txt", 'RSBSPOKE': "RSBSPOKE.txt",
                      'RSBOHDEST':"RSBOHDEST.txt", "RSIS":"RSIS.txt", "RSUPDINFO":"RSUPDINFO.txt",
                      'RSDCUBEMULTI':'RSDCUBEMULTI.txt', 'RSRREPDIR':'RSRREPDIR.txt', 'RSDCUBET':'RSDCUBET.txt',
                      'RSZELTDIR':'RSZELTDIR.txt', 'RSMDATASTATE_EXT':'RSMDATASTATE_EXT.txt', 'RSSTATMANPART': 'RSSTATMANPART.txt'}
    #table_file_map = {'RSSTATMANPART': 'RSSTATMANPART.txt'}
    #t. update_table(table_file_map, unwanted_cols)
    #t.create_flow_table()
    #t.update_flow_from_RSUPDINFO()
    #t.update_flow_from_RSTRAN()
    #t.update_flow_from_RSIOSMAP()
    #t.update_flow_from_RSBSPOKE()
    #t.update_flow_from_RSBOHDEST()
    #t.update_flow_from_RSLDPSEL()
    #t.update_flow_from_RSDCUBEMULTI()
    #t.update_flow_from_RSRREPDIR()
    #TODO General check of data accuracy and possible confusion of ISOURCE, and SOURCE
    #TODO Fix RSBSPOKE assignment of TARGET_TYPE (should be "INFOSPOKE, seems to be OHSOURCE)
    #TODO Add text to datastores
    #TODO the search algorthm probably search routes multiple times which is ineffcient
    #TODO Connections between 1 source and one target
    #graph_file = "BWGraph.svg"
    #t.create_svg_file(t.decorate_graph_flow_volumes(t.create_full_graph()), graph_file)
    forward = True
    graph_file = "BWMiniGraph.svg"
    t.create_svg_file(t.decorate_graph_BI7Flow(t.decorate_graph_flow_volumes(t.create_mini_graph_bwd_fwd('ZC00165'))), graph_file)
    #t.create_svg_file(t.decorate_graph_flow_volumes(t.create_mini_graph_connections('ZO00143')), graph_file)