from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import InvalidRequestError
from sqlalchemy import create_engine
from contextlib import contextmanager
from functools import wraps

BASE = declarative_base()


def get_uri(db_type, db_host_or_path, db_port, db_name, db_user, db_passwd):
    if db_type == "sqlite":
        return f"sqlite:///{db_host_or_path}"
    uri = f"{db_type}://"
    if db_user:
        uri += db_user
        if db_passwd:
            uri += ":" + db_passwd
        uri += "@"
    uri += db_host_or_path
    if db_port:
        uri += ":" + str(db_port)
    if db_name:
        uri += "/" + db_name
    return uri


class SQLConnector:
    def __init__(self, db_type, db_host_or_path, db_port=None, db_name=None, db_user=None, db_passwd=None):
        allowed_types = ("sqlite", "postgresql", "mysql")
        if db_type in allowed_types:
            self.connection_uri = get_uri(db_type, db_host_or_path, db_port, db_name, db_user, db_passwd)
        else:
            raise ValueError(f"{db_type} not in {str(allowed_types)}")

        self.engine = create_engine(self.connection_uri)
        self.Session = sessionmaker(bind=self.engine, autoflush=True)

    def create_tables(self):
        # if not hasattr(self, 'BASE'):
        #     raise AttributeError("You must create the variable 'BASE' in the class definition")
        # self.BASE.metadata.create_all(self.engine)
        BASE.metadata.create_all(self.engine)

    def execute_query(self, query):
        connection = self.engine.connect()
        return connection.execute(query).fetch_all()

    @contextmanager
    def session_scope(self):
        """Provide a transactional scope around a series of operations."""
        session = self.Session()
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


def manage_session(function):
    """Ensure correct session management in transactions"""
    @wraps(function)
    def manager(*args, **kwargs):
        if 'session' not in kwargs:
            with args[0].session_scope() as session:
                kwargs.update({"session": session})
                return function(*args, **kwargs)
        return function(*args, **kwargs)
    return manager
