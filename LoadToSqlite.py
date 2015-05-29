__author__ = 'U104675'
import sqlite3

class TextInSQL():
    def __init__(self, database, file, table, unwanted_cols):
        f = open(file,'r', encoding='Latin-1')
        conn = sqlite3.connect(database)
        c = conn.cursor()
        #Delete table if already exists
        c.execute('DROP TABLE IF EXISTS ' + table)

        unwanted_cols.sort()
        unwanted_cols.reverse() #Need to reverse or col numbers change when do deletions
        curr_row = 0
        #try:
        for line in f:
            if line[0] == '|':
                line_list = line[1:len(line)-2].split('|') # Remove first and last '|' characters
                line_list = remove_cols(line_list, unwanted_cols)
                if curr_row == 0:
                    col_def = [col_names_reformat(el) + ' text' for el in line_list]
                    head_def = [col_names_reformat(el) for el in line_list]
                    sql_stmt = "CREATE TABLE " + table + " (" +  ", ".join(col_def) +")"
                    print(sql_stmt)
                    c.execute(sql_stmt)
                else:
                    sql_stmt = "INSERT INTO " + table + " (" + ",".join(head_def) + ") VALUES (" + ",".join([repr(el.strip().replace("'"," ")) for el in line_list]) +")"
                    try :
                        c.execute(sql_stmt)
                    except (sqlite3.OperationalError, ): print (sql_stmt)
                curr_row += 1
       # except (UnicodeDecodeError):
       #     print ("UnicodeDecodeError: " + sql_stmt)
       #     conn.commit()

        conn.commit()

def remove_cols(line, col_nums):
    for i in col_nums: del line[i]
    return line

def col_names_reformat(in_str):
    el = "".join(x for x in in_str if x.isalnum())
    return "[" + el + "]" # the square bracket allows table names starting in digit - its sqlite quoting mechanism

