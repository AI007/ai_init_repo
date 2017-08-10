from config import config
import pymysql.cursors
import os, os.path
import logging

class create_tables():

    def __init__(self):

        # Open database connection
        self.sql_host = config['MYSQL']['HOST_SQL']
        self.sql_username = config['MYSQL']['USERNAME_SQL']
        self.sql_password = config['MYSQL']['PASSWORD_SQL']
        self.sql_db = config['MYSQL']['DB_SQL']
        self.reconnect()

    def reconnect(self):
        try:
          self.db.close()
        except:
          # Connection might not have been started yet, or timed out
          pass

        self.db = pymysql.connect(host=self.sql_host,
                                  user=self.sql_username,
                                  passwd=self.sql_password, db=self.sql_db,
                                  local_infile=True)

        # Set up cursor
        self.cursor = self.db.cursor()

    def _table_existor(self, tablename):
        # find out if a table with tablename exists in mysql

        squery = '''SHOW TABLES like "%s"''' % (tablename)

        try:
        # Create table
            self.cursor.execute(squery)
            op = self.cursor.fetchall()
            # Commit your changes in the database
            self.db.commit()
        except:
             # Rollback in case there is any error
            self.db.rollback()   
            op = []

        if len(op) == 0 or op[0][0] == None:  # empty table
            exist = 0
        else:
            exist = 1

        return exist

    def create_table(self, createTable, tablename):

        # Reopen the connection to MySQL
        self.reconnect()

        tablename = str(tablename)

        try:
          # Create table
          self.cursor.execute(createTable)

          # Commit your changes in the database
          self.db.commit()

          logging.info("Creating table %s" % tablename)
          
        except:
          # Rollback in case there is any error
          self.db.rollback()          
          logging.info("Error creating table %s" % tablename)


#---------------------------------------------------------------------------
    def _CreateTheTable_AskAnswer_updated(self, tablename):

        table_exists = self._table_existor(tablename)

        if table_exists == 0: 
            createTable = """
                CREATE TABLE IF NOT EXISTS %s \
                (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, \
                input varchar(100) DEFAULT NULL, \
                input_uuid varchar(100) DEFAULT NULL, \
                origin varchar(100) DEFAULT NULL, \
                user_continuation varchar(100) DEFAULT NULL, \
                ask_answer varchar(100) DEFAULT NULL, \
                ask_answer_uuid varchar(100) DEFAULT NULL, \
                created_at datetime DEFAULT NULL, \
                updated_at datetime DEFAULT NULL, \
                conversation_uuid varchar(100) DEFAULT NULL, \
                user_id INT DEFAULT NULL, \
                user_rating INT DEFAULT NULL, \
                question_id varchar(100) DEFAULT NULL
                );
                ;""" % (tablename)
            self.create_table(createTable, tablename)
        else:
            logging.info('table ' + tablename + ' already exists')

