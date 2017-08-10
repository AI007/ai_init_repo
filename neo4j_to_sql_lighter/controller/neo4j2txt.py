from config import config
from py2neo import Graph, authenticate
from py2neo.packages.httpstream import http
from pandas import DataFrame
from pandas.io import sql
from sqlalchemy import create_engine
import pandas as pd 
import itertools
import os, os.path
import py2neo
import time
import logging
import time
import json
import re
import datetime
import pymysql.cursors
import os, os.path
import controller.create_tables as y

create_tables = y.create_tables()

class neo4j2txt(object):

    def __init__(self):
        http.socket_timeout = 9999

        # get login details
        neo4j_url = config['DATABASE']['URL_NEO']
        neo4j_username = config['DATABASE']['USERNAME_NEO']
        neo4j_password = config['DATABASE']['PASSWORD_NEO']

        # set up authentication parameters
        authenticate(neo4j_url, neo4j_username, neo4j_password)

        # Open database connection
        self.sql_host = config['MYSQL']['HOST_SQL']
        self.sql_username = config['MYSQL']['USERNAME_SQL']
        self.sql_password = config['MYSQL']['PASSWORD_SQL']
        self.sql_db = config['MYSQL']['DB_SQL']
        self.reconnect()
        DB_TYPE = 'mysql'
        DB_DRIVER = 'pymysql'
        DB_PORT = 3306

        self.SQLALCHEMY_DATABASE_URI = '%s+%s://%s:%s@%s:%s/%s?charset=utf8' % (DB_TYPE, DB_DRIVER
                                                                , self.sql_username
                                                                , self.sql_password
                                                                , self.sql_host, DB_PORT
                                                                , self.sql_db)

        self.ENGINE = create_engine(self.SQLALCHEMY_DATABASE_URI, pool_size=50, max_overflow=0)

        # connect to authenticated graph database
        self.graph = Graph(config['DATABASE']['ENDPOINT_NEO'])
        self.batch_size = config['BATCHING']['binsize']
        self.startloop = config['BATCHING']['startloop']

        # path to save text output
        path_txt = os.path.dirname(os.path.abspath('./txt/')) + '/txt/'

        # if path doesn't exist then create one
        if not os.path.exists(path_txt):
            os.makedirs(path_txt)

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

    def _get_last_date(self, tablename):
        # gets the last date of the target table
        # tablename: name of database table

        # Reopen the connection to MySQL
        self.reconnect()

        squery = 'select unix_timestamp(max(created_at)) as last_date from {}'.format(tablename)

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
            ndate = '2016-01-01'
            ndate_unix = 1451610000000
        else:
            ndate_unix = op[0][0]*1000
            # convert from unix to time
            ndate = time.strftime('%Y-%m-%d', time.localtime(ndate_unix))

        op = {'ndate':ndate, 'ndate_unix':ndate_unix}
        return op

    def _get_last_date_conditions(self, tablename, column, condition):
        # gets the last date of the target table
        # tablename: name of database table
        # column: column name of database table
        # condition: condition of column name to put in where statement

        # Reopen the connection to MySQL
        self.reconnect()

        squery = 'select unix_timestamp(max(created_at)) as last_date from {} where {} = "{}"'.format(tablename, column, condition)

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
            ndate = '2016-01-01'
            ndate_unix = 1451610000000
        else:
            ndate_unix = op[0][0]*1000
            # convert from unix to time
            ndate = time.strftime('%Y-%m-%d', time.gmtime(ndate_unix))

        op = {'ndate':ndate, 'ndate_unix':ndate_unix}
        return op

    def _append_data_1user(self, op_query, key_set):

        op1 = list(op_query)

        # append cypher outputs to a dictionary for a single user
        blank_list = []
        for record in op1:
            info = {key: record[key] for key in key_set}
            blank_list.append(info)

        return blank_list

    def _clean_string(self, all_conv, prop, n):
        # all_conv = 'all conversations' list
        # prop = string name to clean up
        # n = integer in for loop
        try:
            all_conv[n][prop] = re.sub(r'\W+', ' ', all_conv[n][prop])
            all_conv[n][prop] = re.sub(r'\s+', ' ', all_conv[n][prop])
            # all_conv[n][prop] = all_conv[n][prop].encode('utf-8')
            # all_conv[n][prop] = all_conv[n][prop].decode('utf-8')
        except:
            print(all_conv[n][prop])
            all_conv[n][prop] = 'cannot clean string'

        return all_conv[n][prop]

#-------------------------------------------------------------------------------------------------------
    def _get_AskAnswer_updated(self, tablename):

        ## get last date from table
        d = self._get_last_date(tablename) 

        ndate_unix = d['ndate_unix']

        squery = """
                    match (aa:AskAnswer) - [:RESPONDED_WITH*] -> (c:Conversation) <-[:HAS_CONVERSATION] -(u:User)
                    where toInt(aa.created) > {ndate_unix} + 1000
                    
                    with aa,c,u
                    match path1 = (e_input:Element)-[:RESPONDED_WITH] -> (e_origin:Element) -[:RESPONDED_WITH*]-> (aa)
                    where e_origin.origin is not null  
                           and NONE ( n in filter( i in nodes(path1) where exists(i.origin)) //filter retrieves all the nodes that have the .origin property 
                                      where (n)<-[:RESPONDED_WITH*]-(e_origin) 
                                    ) //no other node containing .origin after e_origin
                    
                    //sometimes AskAnswer is linked directly to e_origin, without e_sure in between (basically: e_origin-[:RESPONDED_WITH]->(:AskAnswer) ). Ex: AskAnswer{uuid:'8a46d360-56bb-11e7-a1a5-005056bd33e3'}
                    //that's why e_sure is retrieved here separately (and is basically the AskAnswer node itself)
                    with aa,c,u,e_input, e_origin
                    match (e_origin)-[:RESPONDED_WITH]->(e_sure) 
                    
                    return e_input.value as input, e_input.uuid as `input_uuid`,
                           //e_origin.value,
                           e_origin.origin as origin,
                           e_sure.value as `user_continuation`,
                    
                           aa.value as `ask_answer`, aa.uuid as `ask_answer_uuid`,
                           aa.created as `created_at`,
                           aa.updated as `updated_at`,
                           
                           c.uuid as `conversation_uuid`,
                           u.user_id as `user_id`
                          
                           //,aa.source
                           ,left( trim( split( aa.source, '"user_rating":')[1] ) ,1) as `user_rating`
                           ,case
                              when aa.source contains '"source_id": null'
                              then 'not existent'
                              else  right(left( trim( split( aa.source, '"source_id":')[1] ) ,7),6) 
                           end as `question_id`
                  """
        op_query = self.graph.run(squery, ndate_unix = ndate_unix)

        key_set = ['input', 'input_uuid', 'origin', 'user_continuation', 'ask_answer'
                   'ask_answer_uuid', 'created_at', 'updated_at', 'conversation_uuid'
                   'user_id', 'user_rating', 'question_id']

        blank_list = self._append_data_1user(op_query, key_set)
        return blank_list


    def save_AskAnswer_updated(self, tablename):
        create_tables._CreateTheTable_AskAnswer_updated(tablename)

        all_conv = []
        key_set = ['input', 'input_uuid', 'origin', 'user_continuation', 'ask_answer'
                   'ask_answer_uuid', 'created_at', 'updated_at', 'conversation_uuid'
                   'user_id', 'user_rating', 'question_id']

        op_cypher = self._get_AskAnswer_updated(tablename)
        all_conv.append(op_cypher)
        all_conv = list(itertools.chain(*all_conv))

        # DateTime instead of Unix
        for n in all_conv:
          n['created_at'] = datetime.datetime.fromtimestamp(int(n['created_at'] / 1000)).strftime('%Y-%m-%d %H:%M:%S')
          n['updated_at'] = datetime.datetime.fromtimestamp(int(n['updated_at'] / 1000)).strftime('%Y-%m-%d %H:%M:%S')

        df = pd.DataFrame(all_conv, columns=key_set)
        df.to_sql(con=self.ENGINE, name=tablename, if_exists='append', flavor='mysql', chunksize=500, index=0)