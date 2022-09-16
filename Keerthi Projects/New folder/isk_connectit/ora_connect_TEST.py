# from multiprocessing import connection
import cx_Oracle
import pandas as pd
# import jprops
from jproperties import Properties


class DatabaseConnection:
    """
    Instantiates a database connection operation.
    Connection credentials will be adjusted accordingly in a specified order establishing successful connection to your database.
    """

    def __init__(self, accessName) -> None:
        self.accessName = accessName

    def accessNameValidation(self) -> str:
        print('Checking for accessName...', self.accessName)
        with open(r'C:\Users\Administrator\PythonDeployment\Class Allocation\Server Files\isk_connectit\jython_DB.properties', 'r') as fp:
            countdata = 0
            fdata = fp.readlines()
            for i in fdata:
                if self.accessName in i:
                    print(i)
                    countdata += 1
            if countdata == 5:
                print('===>', countdata)
                return 'YES'
            else:
                return 'NO'

    def getDBProperties(self):
        with open(r'C:\Users\Administrator\PythonDeployment\Class Allocation\Server Files\isk_connectit\jython_DB.properties', 'rb') as fp:
            db_dict = {}
            properties_caller = Properties()
            properties_caller.load(fp)
            for prop_data in properties_caller.items():
                if prop_data[0].startswith('{}_'.format(self.accessName)):
                    # print(prop_data[0].split('_')[1], ': |', prop_data[1].data)
                    db_dict.update({prop_data[0].split('_')[1]: prop_data[1].data})

            # con_str = """{}/{}@{}:{}/{}""".format(db_dict['USERNAME'], db_dict['PASSWORD'], db_dict['HOST'],
            #                                       db_dict['PORT'], db_dict['SERVICE'])
            print(db_dict)
            return db_dict

    def GetDatabaseProperties(self):
        print('We have accessName now --> {}'.format(self.accessName))

    def MakeConnectionFormat(self, username, password, host, port, service) -> str:
        """
        Prepares a database connection format.

        :param username: Your database username.
        :type username: str

        :param password: Your database password.
        :type password: str

        :param host: Your database host/IP address.
        :type host: str

        :param port: Your database service name.
        :type service: str

        :return: The result of the DatabaseConnection.
        :rtype: str
        """
        print('We have accessName MakeDatabaseConnection now --> {}'.format(self.accessName))
        # return "{}/{}@{}:{}/{}".format(self.username, self.password, self.host, self.port, self.service)
        return "{}/{}@{}:{}/{}".format(username, password, host, port, service)


    def GetConnected(self, connStr : str, connQry = None):
        print(connQry)
        connectionObj = cx_Oracle.connect("{}".format(connStr))
        connectionCursor = connectionObj.cursor()
        dbData = pd.read_sql(connQry, connectionObj)
        connectionCursor.close()
        connectionObj.close()
        return dbData

