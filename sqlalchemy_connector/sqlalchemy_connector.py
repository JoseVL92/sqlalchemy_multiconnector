from contextlib import contextmanager
from functools import wraps

from sqlalchemy import create_engine
from sqlalchemy.exc import InvalidRequestError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

BASE = declarative_base()


def get_uris(db_type, db_host_or_path, db_port, db_name, db_user, db_passwd):
    if not db_type or not db_host_or_path or not db_name:
        raise ValueError("Not enough data")
    if db_type == "sqlite":
        # ensure that no trailing '/' is present
        if db_host_or_path[-1] == '/':
            db_host_or_path = db_host_or_path[:-1]
        uri = f"sqlite:///{db_host_or_path}"
        # return {"default": f"sqlite:///{db_host_or_path}"}
    else:
        uri = f"{db_type}://"
        if db_user:
            uri += db_user
            if db_passwd:
                uri += ":" + db_passwd
            uri += "@"
        uri += db_host_or_path
        if db_port:
            uri += ":" + str(db_port)
    # if db_name is a list and db name 'default' is not specified,
    # the default database would be the first in the db_name list
    if isinstance(db_name, (list, tuple, set)) and len(db_name) > 0:
        uri_dict = {name: uri + "/" + name for name in db_name}
        if 'default' not in uri_dict:
            uri_dict['default'] = uri + "/" + db_name[0]
    elif isinstance(db_name, str):
        uri_dict = {'default': uri + "/" + db_name}
    else:
        raise ValueError("db_name invalid value")
    return uri_dict


class SQLConnector:
    def __init__(self, db_type, db_host_or_path, db_name, db_port=None, db_user=None, db_passwd=None):
        """
        Creates an object with necessary parameters for connecting to a sql database
        :param db_type: One of 'sqlite', 'postgresql' or 'mysql'
        :param db_host_or_path: If db_type=='sqlite', it is the absolute path of the folder containing the file, otherwise it is a hostname or ip
        :param db_port: Port where db server is listening. None if db_type='sqlite'
        :param db_name: If just one database will be used, it is a single db name (a file name if db_name='sqlite').
               If multiple databases, it would be a list of db names or file names.
        :param db_user: Db server login user. None if db_type='sqlite'
        :param db_passwd: Db server login password. None if db_type='sqlite'
        """
        allowed_types = ("sqlite", "postgresql", "mysql")
        if not db_name:
            raise AttributeError("Must specify at least one db_name")
        if db_type in allowed_types:
            if db_type != 'sqlite' and not (db_name and db_user and db_passwd):
                raise AttributeError(f"db_user and db_password must be declared for {db_type}")
            self.connection_uris = get_uris(db_type, db_host_or_path, db_port, db_name, db_user, db_passwd)
        else:
            raise ValueError(f"{db_type} not in {str(allowed_types)}")

        self.engines = {name: create_engine(uri) for name, uri in self.connection_uris.items()}
        self.Session = sessionmaker(autoflush=True)

    def create_tables(self):
        for _, engine in self.engines.items():
            BASE.metadata.create_all(engine)

    def execute_query(self, query, engine_name='default'):
        engine = self.engines.get(engine_name)
        if engine is None:
            raise ValueError(f"No engine with name {engine_name}")
        connection = engine.connect()
        response = connection.execute(query).fetch_all()
        connection.close()
        return response

    @contextmanager
    def session_scope(self, engine_name='default'):
        """Provide a transactional scope around a series of operations."""
        engine = self.engines.get(engine_name)
        if engine is None:
            raise ValueError(f"No engine with name {engine_name}")
        session = self.Session(bind=engine)
        try:
            yield session
            session.commit()
        except InvalidRequestError:
            session.rollback()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def kill(self):
        for engine in self.engines:
            self.engines[engine].dispose()


def manage_session(function):
    """Ensure correct session management in transactions"""
    @wraps(function)
    def manager(*args, **kwargs):
        if 'session' not in kwargs:
            db_name = kwargs.get('db_name', 'default')
            with args[0].session_scope(engine_name=db_name) as session:
                kwargs.update({"session": session})
                return function(*args, **kwargs)
        return function(*args, **kwargs)

    return manager
