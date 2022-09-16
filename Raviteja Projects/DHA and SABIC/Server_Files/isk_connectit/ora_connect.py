# from multiprocessing import connection
import cx_Oracle


class DatabaseConnection:
    """
    Instantiates a database connection operation.
    Connection credentials will be adjusted accordingly in a specified order establishing successful connection to your database.
    """

    def __init__(self, username, password, host, port, service) -> None:
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.service = service

    def MakeConnectionFormat(self, username, password, host, port, service) -> str:
        """
        Prepares a database connection format.

        :param username: Your database username.
        :type username: str

        :param password: Your database password.
        :type password: str

        :param host: Your database host/IP address.
        :type host: str

        :param: Your database service name.
        :type service: str

        :return: The result of the DatabaseConnection.
        :rtype: str
        """
        return "{}/{}@{}:{}/{}".format(self.username, self.password, self.host, self.port, self.service)

    def GetConnected(self, connStr):
        connectionObj = cx_Oracle.connect("{}".format(connStr))
        connectionCursor = connectionObj.cursor()
        dbData = connectionCursor.execute(' select * from dual ').fetchall()
        connectionCursor.close()
        connectionObj.close()
        return dbData


# Instantiate a DatabaseConnection object
connection = DatabaseConnection('DR1024193', 'Pipl#mdrm$93', '172.16.1.61', '1521', 'DR101412')
# connection = DatabaseConnection()


# Call the MakeConnection method
# print(connection.username)
# print(connection.MakeConnectionFormat('DR1024193', 'Pipl#mdrm$93', '172.16.1.61', '1521', 'DR101412'), '\n')
# print(connection.MakeConnectionFormat(connection.username, connection.password, connection.host, connection.port,
#                                       connection.service), '\n')
con_str = connection.MakeConnectionFormat(connection.username, connection.password, connection.host, connection.port,
                                          connection.service)
# print(connection.GetConnected(con_str))


