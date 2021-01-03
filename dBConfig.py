import pandas as pd
import sqlite3
from sqlite3 import Error
import copy
import numpy as np

def create_connection(db_file):
    """ create a database connection to a SQLite database if it doesn't aleardy exist in persistant memory """
    """ if databse in RAM is desired, replace db_file with :memory: """
    conn = None
    dbLoc = "./db/" + db_file
    try:
        conn = sqlite3.connect(dbLoc)
        print('Connected to database')
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()

def loadDatabase(db_file):
    # Once we have a Connection object, we can then create a Cursor object. Cursors allow us to execute SQL queries against a database
    dbLoc = "./db/" + db_file
    conn = sqlite3.connect(dbLoc)
    cur = conn.cursor()

    return conn, cur

def closeDatabase(conn, cur):
    cur.close()
    conn.close()

# NOT SQL Injection safe
# To implement best practices in future
def read_all_from_table(dbName, table):
    create_connection(dbName)
    conn, cursor = loadDatabase(dbName)
    sql = """SELECT  * from """ + "'" + table + "'"
    df = pd.read_sql_query(sql, conn)
    df.drop('index', inplace=True, axis=1)

    closeDatabase(conn, cursor)

    return df

def dF_to_dB(database, df, table):
    # df: dataframe to save
    # table: table name in string
    # conn: connection to database
    create_connection(database)
    conn, cursor = loadDatabase(database)
    df.to_sql(table, conn, if_exists="replace")
    closeDatabase(conn, cursor)

def dB_to_csv(dbName, table, transcFile):
    df = read_all_from_table(dbName, table)
    df.to_csv(transcFile)

def dB_insert(database, table, rowData):
    create_connection(database)
    conn, cursor = loadDatabase(database)
    rowDataStr = [str(entry) for entry in rowData]
    # print(rowDataStr)
    rowDataStr = np.insert(rowDataStr, 0, '0')
    rowDataStr = ','.join(rowDataStr)
    # print("inside db_insert: ",rowDataStr)
    # print('''INSERT INTO ''' + table + ''' VALUES(''' + rowDataStr + ''')''')
    conn.execute('''INSERT INTO ''' + table + ''' VALUES(''' + rowDataStr + ''')''')
    conn.commit()
    closeDatabase(conn, cursor)

def deleteTable(exchangeName, tableName):
    create_connection(exchangeName)
    conn, cursor = loadDatabase(exchangeName)
    sqlMsg = "DROP TABLE " + tableName
    cursor.execute(sqlMsg)
    closeDatabase(conn, cursor)

def fetch_rows_from_DB(databaseName, tableName, numRows):
    create_connection(databaseName)
    conn,cur = loadDatabase(databaseName)
    #
    sql = """SELECT  * from """ + tableName
    #
    df = pd.read_sql_query(sql, conn)
    if numRows < df.shape[0]:
        reqDF = copy.deepcopy(df[df.shape[0]-numRows:])
    else:
        reqDF = copy.deepcopy(df)
    #
    # free memory of df
    del df
    #
    reqDF.drop('index', inplace=True, axis=1) # remove index from database import
    reqDF.reset_index(inplace=True)
    reqDF.drop('index', inplace=True, axis=1) # remove dataframe index after reset_index
    #
    # dates = reqDF["Date"]
    # date_time = dates.str.split(expand=True)
    # dates = date_time[0]
    # times = date_time[1]
    # reqDF["Date"] = dates
    # reqDF["Time"] = times
    # reqDF.set_index("Date", inplace=True)
    #
    return reqDF

if __name__ == '__main__':
    db_file = "test.db"
    # Initializing a database
    create_connection(db_file)

    # Connecting to a database
    connTest, cursorTest = loadDatabase(db_file)