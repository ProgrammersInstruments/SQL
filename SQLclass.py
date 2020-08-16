import pyodbc


class SQL:
    '''
    class for working with SQL. Use pyodbc library
    '''
    def __init__(self, server, database, username, password, table="", driver="{ODBC Driver 17 for SQL Server}"):
        '''
        initial class method
        :param server: str, server name (often localhost)
        :param database: str, database name
        :param username: str, username
        :param password: str, password
        :param table: str, table name
        :param driver: str, your driver string (can take this of .udl file. Check more information in Internet)
        '''

        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.table = table
        self.driver = driver

    def create_connect(self):
        '''
        method for create connection with SQL database.
        :return: None
        '''
        self.con = pyodbc.connect(
            'DRIVER=' + self.driver + ';SERVER=' + self.server + ';DATABASE='
            + self.database + ';UID=' + self.username + ';PWD=' + self.password) #con (connect) - connection variable
        self.cur = self.con.cursor() # cur (cursor) - variable for work with tables

    def change_table(self, table):
        '''
        method for change table
        :param table: str, table name
        :return: None
        '''
        self.table = table

    def change_user(self, name, password):
        '''
        method for change user`s data
        :param name: str, username
        :param password: str, password
        :return: None
        '''
        self.username = name
        self.password = password

    def create_table(self, attributes):
        '''
        method for create new table
        :param attributes: two-dimensional array, with format : [[ColumnName, ColumnType], [ExampleName, varchar(100)]]
        :return: None
        '''
        quest = """CREATE TABLE dbo.""" + self.table + "(" #quest - string SQL create table with dynamic columns
        for attr in attributes:
            quest = quest + attr[0] + " " + attr[1] + " NULL, "
        quest = quest[:len(quest) - 1]
        quest += ")  ON [PRIMARY]"
        self.cur.execute(quest)

    def check_table(self, table=""):
        '''
        method for check existance table with this name
        :param table: str, table name. default use self.table (table name given earlier)
        :return: int, 0 - table is not exist, 1 - table is exist
        '''
        if table == "":
            table = self.table
        self.cur.execute(
            "SELECT COUNT(*) FROM SYS.objects WHERE name='" + table + "'")
        row = self.cur.fetchone()
        if row[0] == 0:
            return 0
        else:
            return 1

    def create_insert_table(self, parametres):
        '''
        method for create string SQL insert with dynamic parameters. empty values will be NULL in SQL
        :param parametres: two-dimensional array, with format:
        [[ColumnName, Value],
        [ExampleColumnName, text],
        [ExampleColumnName, ""]] if there is an empty string in the database there will be a NULL value
        :return: None
        '''
        quest = "INSERT INTO " + self.table + " ("
        values = ") VALUES ("
        for param in parametres:
            if param[1] != "":
                quest += param[0]
                quest += ", "
                values += "?,"
        self.execquest = quest[:len(quest) - 2] + values[:len(values) - 1] + ")" # string for insert in table

    def insert_table(self, parametres):
        '''
        method for insert row in table, with autosave DB
        :param parametres: two-dimensional array, with format:
        [[ColumnName, Value],
        [ExampleColumnName, text],
        [ExampleColumnName, ""]] if there is an empty string in the database there will be a NULL value
        :return: None
        '''
        self.cur.execute(self.execquest, ([Value for ColumnName, Value in parametres if Value != ""]))
        self.con.commit()

    def save(self):
        '''
        method for save DB
        :return: None
        '''
        self.con.commit()

    def get_cur(self):
        '''
        method for take cursor
        :return: object, cursor
        '''
        return self.cur
