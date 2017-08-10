from config import config
import pymysql.cursors
import os, os.path
import logging

class txt2sql():

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

  def _create_and_populate_tables(self, createTable, populateTable, tablename):

    # Reopen the connection to MySQL
    self.reconnect()

    tablename = str(tablename)

    logging.info("Starting creation of table %s" % tablename)

    try:
      # Create table
      self.cursor.execute(createTable)

      # Commit your changes in the database
      self.db.commit()
      
      logging.info("Successful creation of table %s" % tablename)
    except:
      # Rollback in case there is any error
      self.db.rollback()
      
      logging.info("Error creating table %s" % tablename)


    logging.info("Starting population of table %s" % tablename)

    try:
      # Populate table 
      self.cursor.execute(populateTable)

      # Commit your changes in the database
      self.db.commit()
      logging.info("Successful population of %s" % tablename)
      
    except:
      # Rollback in case there is any error
      self.db.rollback()
      
      logging.info("Error populating %s" % tablename)

  def conversation_to_sql(self):
    path_conversation = os.path.dirname(os.path.abspath('./txt/')) + '/txt/' + 'AnatomyOfConversation.txt'
    
    createTable = """
        DROP TABLE IF EXISTS prod_chatbot_conversations;
        CREATE TABLE prod_chatbot_conversations \
        (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, \
        conversation_id VARCHAR(40) NOT NULL, \
        user_id INT, \
        bot_elements INT, \
        user_elements INT, \
        ask_doctor_elements INT, \
        null_elements INT, \
        total_elements INT, \
        conversation_category VARCHAR(30), \
        rating INT);;"""

    populateTable = """     
        LOAD DATA LOCAL INFILE '%s' \
        INTO TABLE prod_chatbot_conversations \
        FIELDS TERMINATED BY ',' \
        OPTIONALLY ENCLOSED BY '"' \
        LINES TERMINATED BY '\n' \
        IGNORE 1 LINES;; """ % (path_conversation)
   
    self._create_and_populate_tables(createTable, populateTable, 'prod_chatbot_conversations')

  def conversation_base_to_sql(self):
    path_conversation = os.path.dirname(os.path.abspath('./txt/')) + '/txt/' + 'conversations.txt'
    
    createTable = """
        DROP TABLE IF EXISTS prod_chatbot_conversations_base;
        CREATE TABLE prod_chatbot_conversations_base \
        (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, \
        user_id INT, \
        conversation_id VARCHAR(40) NOT NULL, \
        created_at datetime DEFAULT NULL, \
        updated_at datetime DEFAULT NULL, \
        length INT);
        ALTER TABLE prod_chatbot_conversations_base ADD INDEX (user_id);
        ALTER TABLE prod_chatbot_conversations_base ADD INDEX (conversation_id);
        ;"""

    populateTable = """     
        LOAD DATA LOCAL INFILE '%s' \
        INTO TABLE prod_chatbot_conversations_base \
        FIELDS TERMINATED BY ',' \
        OPTIONALLY ENCLOSED BY '"' \
        LINES TERMINATED BY '\n' \
        IGNORE 1 LINES;; """ % (path_conversation)
   
    self._create_and_populate_tables(createTable, populateTable, 'prod_chatbot_conversations_base')

  def conversation_summary_element_type_sql(self):
    path_conversation = os.path.dirname(os.path.abspath('./txt/')) + '/txt/' + 'conversation_summaries_element_type.txt'
    
    createTable = """
        DROP TABLE IF EXISTS prod_chatbot_conversations_summary_element_type;
        CREATE TABLE prod_chatbot_conversations_summary_element_type \
        (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, \
        conversation_id VARCHAR(40) NOT NULL, \
        element_type VARCHAR(40) NOT NULL, \
        count INT);
        ALTER TABLE prod_chatbot_conversations_summary_element_type ADD INDEX (conversation_id);
        ;"""

    populateTable = """     
        LOAD DATA LOCAL INFILE '%s' \
        INTO TABLE prod_chatbot_conversations_summary_element_type \
        FIELDS TERMINATED BY ',' \
        OPTIONALLY ENCLOSED BY '"' \
        LINES TERMINATED BY '\n' \
        IGNORE 1 LINES;; """ % (path_conversation)
   
    self._create_and_populate_tables(createTable, populateTable, 'prod_chatbot_conversations_summary_element_type')

  def conversation_summary_speaker_type_sql(self):
    path_conversation = os.path.dirname(os.path.abspath('./txt/')) + '/txt/' + 'conversation_summaries_speaker_type.txt'
    
    createTable = """
        DROP TABLE IF EXISTS prod_chatbot_conversations_summary_speaker_type;
        CREATE TABLE prod_chatbot_conversations_summary_speaker_type \
        (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, \
        conversation_id VARCHAR(40) NOT NULL, \
        speaker VARCHAR(40) NOT NULL, \
        count INT);
        ALTER TABLE prod_chatbot_conversations_summary_speaker_type ADD INDEX (conversation_id);
        ;"""

    populateTable = """     
        LOAD DATA LOCAL INFILE '%s' \
        INTO TABLE prod_chatbot_conversations_summary_speaker_type \
        FIELDS TERMINATED BY ',' \
        OPTIONALLY ENCLOSED BY '"' \
        LINES TERMINATED BY '\n' \
        IGNORE 1 LINES;; """ % (path_conversation)
   
    self._create_and_populate_tables(createTable, populateTable, 'prod_chatbot_conversations_summary_speaker_type')

  def element_to_sql(self):
    path_element = os.path.dirname(os.path.abspath('./txt/')) + '/txt/' + 'AnatomyOfElement.txt'

    createTable = """
         DROP TABLE IF EXISTS prod_chatbot_elements;
         CREATE TABLE prod_chatbot_elements \
         (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, \
         user_id INT, \
         uuid VARCHAR(40) NOT NULL, \
         conversation_id VARCHAR(40) NOT NULL, \
         bot_user VARCHAR(10) CHARACTER SET utf8, \
         text LONGTEXT, \
         timestamp DATETIME NOT NULL, \
         element_type VARCHAR(30)
         );
         ALTER TABLE prod_chatbot_elements ADD INDEX (user_id);
         ALTER TABLE prod_chatbot_elements ADD INDEX (uuid);
         ALTER TABLE prod_chatbot_elements ADD INDEX (conversation_id);
         ;"""

    populateTable = """
         LOAD DATA LOCAL INFILE '%s' \
         INTO TABLE prod_chatbot_elements \
         FIELDS TERMINATED BY ',' \
         OPTIONALLY ENCLOSED BY '"' \
         LINES TERMINATED BY '\n' \
         IGNORE 1 LINES;;""" % (path_element)
    
    self._create_and_populate_tables(createTable, populateTable, 'prod_chatbot_elements')

  def user_sql(self):
    path_conversation = os.path.dirname(os.path.abspath('./txt/')) + '/txt/' + 'users.txt'

    createTable = """
        DROP TABLE IF EXISTS prod_chatbot_users;
        CREATE TABLE prod_chatbot_users \
        (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, \
        user_id INT, \
        gender VARCHAR(40) DEFAULT NULL, \
        date_of_birth VARCHAR(40) DEFAULT NULL, \
        created_at datetime DEFAULT NULL, \
        updated_at datetime DEFAULT NULL, \
        Country VARCHAR(40) DEFAULT NULL
        );
        ALTER TABLE prod_chatbot_users ADD INDEX (user_id);
        ALTER TABLE prod_chatbot_users ADD INDEX (gender);
        ALTER TABLE prod_chatbot_users ADD INDEX (Country);        
        ;"""


    populateTable = """     
        LOAD DATA LOCAL INFILE '%s' \
        INTO TABLE prod_chatbot_users \
        FIELDS TERMINATED BY ',' \
        OPTIONALLY ENCLOSED BY '"' \
        LINES TERMINATED BY '\n' \
        IGNORE 1 LINES;; """ % (path_conversation)

    self._create_and_populate_tables(createTable, populateTable, 'prod_chatbot_users')

  def ask_sql_RETIRED(self):
    path_conversation = os.path.dirname(os.path.abspath('./txt/')) + '/txt/' + 'ask.txt'

    createTable = """
        DROP TABLE IF EXISTS prod_chatbot_audit_ask;
        CREATE TABLE prod_chatbot_audit_ask \
        (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, \
        question_id INT DEFAULT NULL, \
        user_id INT DEFAULT NULL, \
        created_at datetime DEFAULT NULL, \
        updated_at datetime DEFAULT NULL, \
        element_id VARCHAR(40) DEFAULT NULL, \
        input TEXT DEFAULT NULL, \
        output TEXT DEFAULT NULL, \
        origin VARCHAR(120) DEFAULT NULL, \
        user_rating VARCHAR(140) DEFAULT NULL, \
        element_id_of_question varchar(100) DEFAULT NULL, \
        uuid_of_question varchar(100) DEFAULT NULL
        );;"""

    populateTable = """     
        LOAD DATA LOCAL INFILE '%s' \
        INTO TABLE prod_chatbot_audit_ask \
        FIELDS TERMINATED BY ',' \
        OPTIONALLY ENCLOSED BY '"' \
        LINES TERMINATED BY '\n' \
        IGNORE 1 LINES;; """ % (path_conversation)

  def ask_sql(self):
    path_conversation = os.path.dirname(os.path.abspath('./txt/')) + '/txt/' + 'ask.txt'

    createTable = """
        DROP TABLE IF EXISTS prod_chatbot_audit_ask;
        CREATE TABLE prod_chatbot_audit_ask \
        (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, \
        question_id INT DEFAULT NULL, \
        user_id INT DEFAULT NULL, \
        created_at datetime DEFAULT NULL, \
        updated_at datetime DEFAULT NULL, \
        element_id VARCHAR(40) DEFAULT NULL, \
        input TEXT DEFAULT NULL, \
        output TEXT DEFAULT NULL, \
        input_original TEXT DEFAULT NULL, \
        origin VARCHAR(120) DEFAULT NULL, \
        user_rating VARCHAR(140) DEFAULT NULL, \
        element_id_of_question varchar(100) DEFAULT NULL, \
        uuid_of_question varchar(100) DEFAULT NULL, \
        element_id_of_question_original varchar(100) DEFAULT NULL, \
        uuid_of_question_original varchar(100) DEFAULT NULL        
        );
        ALTER TABLE prod_chatbot_audit_ask ADD INDEX (user_id);
        ALTER TABLE prod_chatbot_audit_ask ADD INDEX (question_id);
        ALTER TABLE prod_chatbot_audit_ask ADD INDEX (uuid_of_question_original);
        ;"""

    populateTable = """     
        LOAD DATA LOCAL INFILE '%s' \
        INTO TABLE prod_chatbot_audit_ask \
        FIELDS TERMINATED BY ',' \
        OPTIONALLY ENCLOSED BY '"' \
        LINES TERMINATED BY '\n' \
        IGNORE 1 LINES;; """ % (path_conversation)

    self._create_and_populate_tables(createTable, populateTable, 'prod_chatbot_audit_ask')

  def leaflet_sql(self):
    path_conversation = os.path.dirname(os.path.abspath('./txt/')) + '/txt/' + 'leaflet.txt'

    createTable = """
        DROP TABLE IF EXISTS prod_chatbot_audit_leaflet;
        CREATE TABLE prod_chatbot_audit_leaflet \
        (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, \
        user_id INT DEFAULT NULL, \
        created_at datetime DEFAULT NULL, \
        updated_at datetime DEFAULT NULL, \
        element_id VARCHAR(40) DEFAULT NULL, \
        input TEXT DEFAULT NULL, \
        output TEXT DEFAULT  NULL, \
        summary TEXT DEFAULT NULL, \
        provider TEXT DEFAULT NULL, \
        source_id TEXT DEFAULT NULL, \
        origin TEXT DEFAULT NULL, \
        useful VARCHAR(40) DEFAULT NULL, \
        element_id_of_question varchar(100) DEFAULT NULL, \
        uuid_of_question varchar(100) DEFAULT NULL, \
        leaflet_type varchar(100) DEFAULT NULL
        );
        ALTER TABLE prod_chatbot_audit_leaflet ADD INDEX (user_id);
        ALTER TABLE prod_chatbot_audit_leaflet ADD INDEX (uuid_of_question);        
        ;"""

    populateTable = """     
        LOAD DATA LOCAL INFILE '%s' \
        INTO TABLE prod_chatbot_audit_leaflet \
        FIELDS TERMINATED BY ',' \
        OPTIONALLY ENCLOSED BY '"' \
        LINES TERMINATED BY '\n' \
        IGNORE 1 LINES;; """ % (path_conversation)

    self._create_and_populate_tables(createTable, populateTable, 'prod_chatbot_audit_leaflet')

  def check_sql_RETIRED(self):
    path_conversation = os.path.dirname(os.path.abspath('./txt/')) + '/txt/' + 'check.txt'

    createTable = """
        DROP TABLE IF EXISTS prod_chatbot_audit_check;
        CREATE TABLE prod_chatbot_audit_check \
        (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, \
        user_id INT, \
        created_at datetime DEFAULT NULL, \
        updated_at datetime DEFAULT NULL, \
        element_id VARCHAR(40) DEFAULT NULL, \
        input TEXT DEFAULT NULL, \
        output TEXT DEFAULT NULL, \
        flow_class VARCHAR(40) DEFAULT NULL, \
        flow_id VARCHAR(40) DEFAULT NULL, \
        num_questions INT, \
        check_duration INT, \
        origin TEXT DEFAULT NULL, \
        user_rating VARCHAR(40) DEFAULT NULL, \
        category varchar(120) DEFAULT NULL, \
        version varchar(40) DEFAULT NULL, \
        element_id_of_question varchar(100) DEFAULT NULL, \
        uuid_of_question varchar(100) DEFAULT NULL
        );;"""

    populateTable = """     
        LOAD DATA LOCAL INFILE '%s' \
        INTO TABLE prod_chatbot_audit_check \
        FIELDS TERMINATED BY ',' \
        OPTIONALLY ENCLOSED BY '"' \
        LINES TERMINATED BY '\n' \
        IGNORE 1 LINES;; """ % (path_conversation)

    self._create_and_populate_tables(createTable, populateTable, 'prod_chatbot_audit_check')

  def check_comments_sql(self):
    path_conversation = os.path.dirname(os.path.abspath('./txt/')) + '/txt/' + 'check_comments.txt'

    createTable = """
        DROP TABLE IF EXISTS prod_chatbot_audit_check_comments;
        CREATE TABLE prod_chatbot_audit_check_comments \
        (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, \
        user_id INT, \
        conversation_id VARCHAR(40) DEFAULT NULL, \
        uuid VARCHAR(40) DEFAULT NULL, \
        created_at datetime DEFAULT NULL, \
        updated_at datetime DEFAULT NULL, \
        user_rating VARCHAR(40) DEFAULT NULL, \
        comment TEXT DEFAULT NULL, \
        feedback_type TEXT DEFAULT NULL
        );
        ALTER TABLE prod_chatbot_audit_check_comments ADD INDEX (user_id);
        ALTER TABLE prod_chatbot_audit_check_comments ADD INDEX (conversation_id);
        ALTER TABLE prod_chatbot_audit_check_comments ADD INDEX (uuid);
        ;"""

    populateTable = """     
        LOAD DATA LOCAL INFILE '%s' \
        INTO TABLE prod_chatbot_audit_check_comments \
        FIELDS TERMINATED BY ',' \
        OPTIONALLY ENCLOSED BY '"' \
        LINES TERMINATED BY '\n' \
        IGNORE 1 LINES;; """ % (path_conversation)

    self._create_and_populate_tables(createTable, populateTable, 'prod_chatbot_audit_check_comments')

  def ask_comments_sql(self):
    path_conversation = os.path.dirname(os.path.abspath('./txt/')) + '/txt/' + 'ask_comments.txt'

    createTable = """
        DROP TABLE IF EXISTS prod_chatbot_audit_ask_comments;
        CREATE TABLE prod_chatbot_audit_ask_comments \
        (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, \
        user_id INT, \
        conversation_id VARCHAR(40) DEFAULT NULL, \
        uuid VARCHAR(40) DEFAULT NULL, \
        created_at datetime DEFAULT NULL, \
        updated_at datetime DEFAULT NULL, \
        user_rating VARCHAR(40) DEFAULT NULL, \
        comment TEXT DEFAULT NULL, \
        question_id varchar(100) DEFAULT NULL
        );
        ALTER TABLE prod_chatbot_audit_ask_comments ADD INDEX (user_id);
        ALTER TABLE prod_chatbot_audit_ask_comments ADD INDEX (conversation_id);
        ALTER TABLE prod_chatbot_audit_ask_comments ADD INDEX (uuid);
        ;"""

    populateTable = """     
        LOAD DATA LOCAL INFILE '%s' \
        INTO TABLE prod_chatbot_audit_ask_comments \
        FIELDS TERMINATED BY ',' \
        OPTIONALLY ENCLOSED BY '"' \
        LINES TERMINATED BY '\n' \
        IGNORE 1 LINES;; """ % (path_conversation)

    self._create_and_populate_tables(createTable, populateTable, 'prod_chatbot_audit_ask_comments')

  def origins_sql(self):
    path_conversation = os.path.dirname(os.path.abspath('./txt/')) + '/txt/' + 'origins.txt'

    createTable = """
        DROP TABLE IF EXISTS prod_chatbot_origins;
        CREATE TABLE prod_chatbot_origins \
        (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, \
        user_id INT, \
        created_at datetime DEFAULT NULL, \
        updated_at datetime DEFAULT NULL, \
        input_question TEXT DEFAULT NULL, \
        response TEXT DEFAULT NULL, \
        response_afterwards TEXT DEFAULT NULL, \
        uuid_of_question varchar(100) DEFAULT NULL, \
        element_id_of_question varchar(100) DEFAULT NULL, \
        origin varchar(100) DEFAULT NULL, \
        origin_category varchar(100) DEFAULT NULL, \
        conversation_id varchar(100) DEFAULT NULL        
        );
        ALTER TABLE prod_chatbot_origins ADD INDEX (user_id);
        ALTER TABLE prod_chatbot_origins ADD INDEX (uuid_of_question);
        ALTER TABLE prod_chatbot_origins ADD INDEX (conversation_id);
        """

    populateTable = """     
        LOAD DATA LOCAL INFILE '%s' \
        INTO TABLE prod_chatbot_origins \
        FIELDS TERMINATED BY ',' \
        OPTIONALLY ENCLOSED BY '"' \
        LINES TERMINATED BY '\n' \
        IGNORE 1 LINES;; """ % (path_conversation)

    self._create_and_populate_tables(createTable, populateTable, 'prod_chatbot_origins')

  def check_sql(self):
    path_conversation = os.path.dirname(os.path.abspath('./txt/')) + '/txt/' + 'check.txt'

    createTable = """
        DROP TABLE IF EXISTS prod_chatbot_audit_check;
        CREATE TABLE prod_chatbot_audit_check \
        (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, \
        user_id INT, \
        created_at datetime DEFAULT NULL, \
        updated_at datetime DEFAULT NULL, \
        element_id VARCHAR(40) DEFAULT NULL, \
        input TEXT DEFAULT NULL, \
        output TEXT DEFAULT NULL, \
        flow_class VARCHAR(40) DEFAULT NULL, \
        flow_id VARCHAR(40) DEFAULT NULL, \
        num_questions INT, \
        check_duration INT, \
        origin TEXT DEFAULT NULL, \
        user_rating VARCHAR(40) DEFAULT NULL, \
        category varchar(120) DEFAULT NULL, \
        version varchar(40) DEFAULT NULL, \
        element_id_of_question varchar(100) DEFAULT NULL, \
        uuid_of_question varchar(100) DEFAULT NULL, \
        uuid_of_outcome varchar(100) DEFAULT NULL
        );
        ALTER TABLE prod_chatbot_audit_check ADD INDEX (user_id);
        ALTER TABLE prod_chatbot_audit_check ADD INDEX (uuid_of_question);
        ALTER TABLE prod_chatbot_audit_check ADD INDEX (uuid_of_outcome);
        ALTER TABLE prod_chatbot_audit_check ADD INDEX (flow_id);
        ;"""

    populateTable = """     
        LOAD DATA LOCAL INFILE '%s' \
        INTO TABLE prod_chatbot_audit_check \
        FIELDS TERMINATED BY ',' \
        OPTIONALLY ENCLOSED BY '"' \
        LINES TERMINATED BY '\n' \
        IGNORE 1 LINES;; """ % (path_conversation)

    self._create_and_populate_tables(createTable, populateTable, 'prod_chatbot_audit_check')

  def triage_sql(self):
    path_conversation = os.path.dirname(os.path.abspath('./txt/')) + '/txt/' + 'triage.txt'

    createTable = """
        DROP TABLE IF EXISTS prod_chatbot_audit_triage;
        CREATE TABLE prod_chatbot_audit_triage \
        (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, \
        user_id INT, \
        created_at datetime DEFAULT NULL, \
        updated_at datetime DEFAULT NULL, \
        flow_id VARCHAR(40) DEFAULT NULL, \
        uuid varchar(100) DEFAULT NULL, \
        flow_name varchar(100) DEFAULT NULL, \
        flow_class_id varchar(100) DEFAULT NULL, \
        flow_class varchar(100) DEFAULT NULL, \
        version varchar(100) DEFAULT NULL, \
        title varchar(100) DEFAULT NULL, \
        category varchar(100) DEFAULT NULL, \
        num_questions INT
        );
        ALTER TABLE prod_chatbot_audit_triage ADD INDEX (user_id);
        ALTER TABLE prod_chatbot_audit_triage ADD INDEX (flow_id);
        ;"""

    populateTable = """     
        LOAD DATA LOCAL INFILE '%s' \
        INTO TABLE prod_chatbot_audit_triage \
        FIELDS TERMINATED BY ',' \
        OPTIONALLY ENCLOSED BY '"' \
        LINES TERMINATED BY '\n' \
        IGNORE 1 LINES;; """ % (path_conversation)

    self._create_and_populate_tables(createTable, populateTable, 'prod_chatbot_audit_triage')

  def user2_sql(self):
    path_conversation = os.path.dirname(os.path.abspath('./txt/')) + '/txt/' + 'users2.txt'

    createTable = """
        DROP TABLE IF EXISTS prod_chatbot_users2;
        CREATE TABLE prod_chatbot_users2 \
        (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, \
        user_id INT, \
        gender VARCHAR(40) DEFAULT NULL, \
        date_of_birth VARCHAR(40) DEFAULT NULL, \
        created_at datetime DEFAULT NULL, \
        updated_at datetime DEFAULT NULL, \
        Country VARCHAR(40) DEFAULT NULL
        );
        ALTER TABLE prod_chatbot_users2 ADD INDEX (user_id);
        ALTER TABLE prod_chatbot_users2 ADD INDEX (gender);
        ALTER TABLE prod_chatbot_users2 ADD INDEX (Country);        
        ;"""

    populateTable = """     
        LOAD DATA LOCAL INFILE '%s' \
        INTO TABLE prod_chatbot_users2 \
        FIELDS TERMINATED BY ',' \
        OPTIONALLY ENCLOSED BY '"' \
        LINES TERMINATED BY '\n' \
        IGNORE 1 LINES;; """ % (path_conversation)

    self._create_and_populate_tables(createTable, populateTable, 'prod_chatbot_users2')

  def checkbase_sql(self):
    path_conversation = os.path.dirname(os.path.abspath('./txt/')) + '/txt/' + 'checkbase.txt'

    createTable = """
        DROP TABLE IF EXISTS prod_chatbot_audit_checkbase;
        CREATE TABLE prod_chatbot_audit_checkbase \
        (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, \
        user_id INT, \
        created_at datetime DEFAULT NULL, \
        updated_at datetime DEFAULT NULL, \
        element_id VARCHAR(40) DEFAULT NULL, \
        input TEXT DEFAULT NULL, \
        output TEXT DEFAULT NULL, \
        flow_class VARCHAR(40) DEFAULT NULL, \
        flow_id VARCHAR(40) DEFAULT NULL, \
        num_questions INT, \
        check_duration INT, \
        origin TEXT DEFAULT NULL, \
        user_rating VARCHAR(40) DEFAULT NULL, \
        category varchar(120) DEFAULT NULL, \
        version varchar(40) DEFAULT NULL, \
        element_id_of_question varchar(100) DEFAULT NULL, \
        uuid_of_question varchar(100) DEFAULT NULL, \
        uuid_of_outcome varchar(100) DEFAULT NULL
        );
        ALTER TABLE prod_chatbot_audit_checkbase ADD INDEX (user_id);
        ALTER TABLE prod_chatbot_audit_checkbase ADD INDEX (uuid_of_question);
        ALTER TABLE prod_chatbot_audit_checkbase ADD INDEX (uuid_of_outcome);
        ALTER TABLE prod_chatbot_audit_checkbase ADD INDEX (flow_id);
        ;"""

    populateTable = """     
        LOAD DATA LOCAL INFILE '%s' \
        INTO TABLE prod_chatbot_audit_checkbase \
        FIELDS TERMINATED BY ',' \
        OPTIONALLY ENCLOSED BY '"' \
        LINES TERMINATED BY '\n' \
        IGNORE 1 LINES;; """ % (path_conversation)

    self._create_and_populate_tables(createTable, populateTable, 'prod_chatbot_audit_checkbase')
