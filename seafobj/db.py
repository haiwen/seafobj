import os
import ConfigParser
import logging

from urllib import quote_plus

from sqlalchemy import create_engine
from sqlalchemy.event import contains as has_event_listener, listen as add_event_listener
from sqlalchemy.exc import DisconnectionError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import Pool
from sqlalchemy.orm.scoping import scoped_session
from sqlalchemy.ext.automap import automap_base

# Automatically generate mapped classes and relationships from a database schema
Base = automap_base()

def create_engine_from_conf(config):
    need_connection_pool_fix = True

    if not config.has_section('database'):
        seafile_data_dir = os.environ['SEAFILE_CONF_DIR']
        if seafile_data_dir:
            path = os.path.join(seafile_data_dir, 'seafile.db')
        else:
            logging.warning('SEAFILE_CONF_DIR not set, can not load sqlite database.')
            return None
        db_url = "sqlite:///%s" % path
        logging.info('[seafobj] database: sqlite3, path: %s', path)
        need_connection_pool_fix = False
    else:
        backend = config.get('database', 'type')

        if backend == 'mysql':
            if config.has_option('database', 'host'):
                host = config.get('database', 'host').lower()
            else:
                host = 'localhost'

            if config.has_option('database', 'port'):
                port = config.getint('database', 'port')
            else:
                port = 3306
            username = config.get('database', 'user')
            passwd = config.get('database', 'password')
            dbname = config.get('database', 'db_name')
            db_url = "mysql+mysqldb://%s:%s@%s:%s/%s?charset=utf8" % (username, quote_plus(passwd), host, port, dbname)
            logging.warning('[seafobj] database: mysql, name: %s', dbname)
        elif backend == 'oracle':
            if config.has_option('database', 'host'):
                host = config.get('database', 'host').lower()
            else:
                host = 'localhost'

            if config.has_option('database', 'port'):
                port = config.getint('database', 'port')
            else:
                port = 1521
            username = config.get('database', 'username')
            passwd = config.get('database', 'password')
            service_name = config.get('database', 'service_name')
            db_url = "oracle://%s:%s@%s:%s/%s" % (username, quote_plus(passwd),
                    host, port, service_name)

            logging.info('[seafobj] database: oracle, service_name: %s', service_name)
        else:
            raise RuntimeError("Unknown database backend: %s" % backend)

    # Add pool recycle, or mysql connection will be closed by mysqld if idle
    # for too long.
    kwargs = dict(pool_recycle=300, echo=False, echo_pool=False)

    engine = create_engine(db_url, **kwargs)

    if need_connection_pool_fix and not has_event_listener(Pool, 'checkout', ping_connection):
        # We use has_event_listener to double check in case we call create_engine
        # multipe times in the same process.
        add_event_listener(Pool, 'checkout', ping_connection)

    return engine

def init_db_session_class(config):
    """Configure Session class for mysql according to the config file."""
    try:
        engine = create_engine_from_conf(config)
    except ConfigParser.NoOptionError, ConfigParser.NoSectionError:
        raise RuntimeError("invalid seafile config.")

    # reflect the tables
    Base.prepare(engine, reflect=True)

    Session = sessionmaker(bind=engine)
    return Session

# This is used to fix the problem of "MySQL has gone away" that happens when
# mysql server is restarted or the pooled connections are closed by the mysql
# server beacause being idle for too long.
#
# See http://stackoverflow.com/a/17791117/1467959
def ping_connection(dbapi_connection, connection_record, connection_proxy): # pylint: disable=unused-argument
    cursor = dbapi_connection.cursor()
    try:
        cursor.execute("SELECT 1")
        cursor.close()
    except:
        logging.info('fail to ping database server, disposing all cached connections')
        connection_proxy._pool.dispose() # pylint: disable=protected-access

        # Raise DisconnectionError so the pool would create a new connection
        raise DisconnectionError()
