import mysql.connector
from mysql.connector import Error

class Connection:
    def __init__(self):

        self.connection = mysql.connector.connect(host='localhost',
                                         database='all_nft_data',
                                         user='root',
                                         password='YOURPASSWORD')
        self.connect = self.connection.cursor()


    def InsertData(data):

        connection = Connection()
        connection.connect.execute(data)
        connection.connection.commit()
        connection.connect.close()
        return connection
        
    def UpdateData(data):
        connection = Connection()
        connection.connect.execute(data)
        connection.connection.commit()
        connection.connect.close()
        

        return connection


    def selectData(data):
        connection = Connection()
        # query = f"select * from {self.tableName};"
        connection.connect.execute(data)

        myresult = connection.connect.fetchall()
        connection.connect.close()
        return myresult
