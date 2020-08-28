#############################################################################
#############################################################################

from pandas import DataFrame
from psycopg2 import DatabaseError
from configparser import ConfigParser
from sqlalchemy import create_engine

#############################################################################
#############################################################################

def psqlConfig(filename, section = 'postgresql'):
    parser = ConfigParser()
    parser.read(filename)
    database = {}
    if parser.has_section(section):
        parameters = parser.items(section)
        for parameter in parameters:
            database[parameter[0]] = parameter[1]
    else:
        raise Exception('Section {0} not found in the {1} file.'.format(section, filename))
    return database

def psqlEngine(filename, section = 'postgresql'):
    connection = None
    try:
        parameters = psqlConfig(filename = filename, section = section)
        host, database = parameters['host'], parameters['database']
        user, password = parameters['user'], parameters['password']
        engine = create_engine('postgresql://{}:{}@{}:5432/{}'.format(user, password, host, database))
    except(Exception, DatabaseError) as error:
        engine = None
        print(error)
    return engine
