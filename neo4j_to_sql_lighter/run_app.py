import logging
from logging.config import fileConfig
fileConfig('logging.conf')
logger = logging.getLogger()

import controller.neo4j2txt as neo4j2txt
neo4j2txt.neo4j2txt().save_AskAnswer_updated('prod_chatbot_audit_ask_updated') #written by AI on 10/August/2017