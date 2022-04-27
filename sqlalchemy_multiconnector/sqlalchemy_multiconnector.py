from datetime import datetime

from contextlib import contextmanager
from functools import wraps

from sqlalchemy import create_engine
from sqlalchemy.engine.row import Row
from sqlalchemy.exc import InvalidRequestError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, class_mapper

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


def manage_session(function):
    """Ensure correct session management in transactions"""

    @wraps(function)
    def manager(*args, **kwargs):
        if 'session' not in kwargs:
            db_name = kwargs.get('db_name') or 'default'
            schema_name = kwargs.get('schema_name')
            with args[0].session_scope(engine_name=db_name, schema_name=schema_name) as session:
                kwargs.update({"session": session})
                return function(*args, **kwargs)
        return function(*args, **kwargs)

    return manager


def to_dict(obj, found=None, recursive=False):
    if isinstance(obj, Row):
        return obj._asdict()
    if found is None:
        found = set()
    mapper = class_mapper(obj.__class__)
    columns = [column.key for column in mapper.columns]
    get_key_value = lambda c: (c, getattr(obj, c).isoformat()) if isinstance(getattr(obj, c), datetime) else (
        c, getattr(obj, c))
    out = dict(map(get_key_value, columns))
    if recursive:
        for name, relation in mapper.relationships.items():
            if relation not in found:
                found.add(relation)
                related_obj = getattr(obj, name)
                if related_obj is not None:
                    if relation.uselist:
                        out[name] = [to_dict(child, found, True) for child in related_obj]
                    else:
                        out[name] = to_dict(related_obj, found, True)
    return out


class SQLConnector:
    def __init__(self, db_type, db_host_or_path, db_name, db_port=None, db_schemas=None, db_user=None, db_passwd=None,
                 session_autoflush=True, session_autocommit=False):
        """
        Creates an object with necessary parameters for connecting to a sql database
        :param db_type: One of 'sqlite', 'postgresql' or 'mysql'
        :param db_host_or_path: If db_type=='sqlite', it is the absolute path of the folder containing the file, otherwise it is a hostname or ip
        :param db_name: If just one database will be used, it is a single db name (a file name if db_name='sqlite').
               If multiple databases, it would be a list of db names or file names.
        :param db_port: Port where db server is listening. None if db_type='sqlite'
        :param db_schemas: List of schemas used on every specified database
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

        self.schemas = db_schemas if not db_type == 'sqlite' else None
        if isinstance(self.schemas, str):
            self.schemas = [self.schemas]

        self.engines = {
            name: create_engine(uri) for name, uri in self.connection_uris.items()
        }
        self.Session = sessionmaker(autoflush=session_autoflush, autocommit=session_autocommit)

    def create_tables(self, schemas=None):
        schemas = schemas or self.schemas
        if isinstance(schemas, str):
            schemas = [schemas]
        self._create_schemas(schemas)
        for _, engine in self.engines.items():
            if schemas is not None:
                for sc in self.schemas:
                    BASE.metadata.create_all(
                        bind=engine.connect().execution_options(
                            schema_translate_map={None: sc}
                        )
                    )
            else:
                BASE.metadata.create_all(engine)

    def _create_schemas(self, schemas=None):
        schemas = schemas or self.schemas
        if schemas is None:
            return
        if isinstance(schemas, str):
            schemas = [schemas]
        for engine_name, _ in self.engines.items():
            for sc in schemas:
                self.execute_query("CREATE SCHEMA IF NOT EXISTS " + sc, engine_name)

    def execute_query(self, query, engine_name='default'):
        """Execute a raw query on database 'engine_name'.
        If any schema will be used, it must be specified in the sql statement"""
        engine = self.engines.get(engine_name)
        if engine is None:
            raise ValueError(f"No engine with name {engine_name}")
        connection = engine.connect()
        response = connection.execute(query)
        returnable = False
        if hasattr(response, '.fetch_all()'):
            response = response.fetch_all()
            returnable = True
        connection.close()
        if returnable:
            return response

    @manage_session
    def create_resource(self, resource_orm_class, *, return_id=False, session=None, **kwargs):
        """
        Add a resource. Doesn't check for integrity errors. Valid for resources without foreign keys.
        :param resource_orm_class: ORM class related to the resource
        :param session: Session to be used to execute query
        :param return_id: If it needs to commit this query to catch the new autocreated 'id' and returning it
        :return: True (or element 'id' if return_id is True) if the operation succeeded
        """
        element = resource_orm_class(**kwargs)
        session.add(element)
        if return_id:
            session.flush()
            session.refresh(element)
            return element.id
        return True

    @manage_session
    def delete_resource(self, resource_orm_class, pk, *, session=None):
        """
        Deletes a resource
        :param resource_orm_class: ORM class related to the resource
        :param pk: Primary key value
        :param session: Session to be used to execute query
        :return: True if the operation succeeded
        """
        resource = session.query(resource_orm_class).get(pk)
        if resource is not None:
            session.delete(resource)
        return True

    @manage_session
    def get_resource(self, resource_orm_class, pk, pk_fieldname=None, fields=None, *,
                     session=None, check_existence=True):
        """
        Get details about a specific resource. Fields selection is only allowed if pk_fieldname is specified.
        :param resource_orm_class: ORM class related to the resource
        :param pk: Primary key value
        :param pk_fieldname: Primary key column name.
               If not present, a 'query.get' clause will be used and 'fields' parameter will be ignored'
        :param fields: Desired columns to be returned. If pk_fieldname is None, it will be ignored
        :param session: Session to be used to execute query
        :param check_existence: If this method is invoked just to check resource existence
        :return: A dictionary with the resource information
        :raise: ValueError if no resource with 'pk' primary key value is found
        """
        if not pk_fieldname:
            resource = session.query(resource_orm_class).get(pk)
        else:
            if fields:
                fields = [getattr(resource_orm_class, f) for f in fields]
            else:
                fields = [resource_orm_class]
            resource = session.query(*fields).filter(getattr(resource_orm_class, pk_fieldname) == pk).one_or_none()
        if check_existence:
            return resource is not None
        if resource is None:
            raise ValueError(f"Resource '{resource_orm_class.__tablename__}' with pk='{pk}' not found")
        return to_dict(resource)

    @manage_session
    def list_resources(self, resource_orm_class, resource_query_binding_class, filter_and_sort_dict=None,
                       fields=None, limit=1000, offset=0, *, session=None):
        """
        Get a list of resources that meet a set of parameters
        :param resource_orm_class: ORM class related to the resource
        :param resource_query_binding_class: QueryBinding-based class (from 'sqlalchemy-filterparams')
        :param filter_and_sort_dict: Dictionary of options specified by 'filterparams' library
        :param fields: Columns to be selected
        :param limit: Max number of rows fetched
        :param offset: Number of rows to skip before starting to return rows from the query
        :param session: Session to be used to execute the query
        :return: A dictionary with shape {"total": total_count, "elements": [elements_list]}
        """
        if limit > 1000:
            raise ValueError("Limit out of bounds")
        if filter_and_sort_dict:
            query = resource_query_binding_class(session=session).evaluate_params(filter_and_sort_dict)
        else:
            query = session.query(resource_orm_class)

        if fields:
            columns = [getattr(resource_orm_class, f) for f in fields]
            query = query.with_entities(*columns)

        total_count = 0
        if limit or offset:
            total_count = query.count()

        # slice operation was kept with documentation purposes
        if limit and offset:
            end_index = offset + limit
            query = query.slice(offset, end_index)
        elif limit:
            query = query.limit(limit)
        elif offset:
            query = query.offset(offset)

        elements_list = query.all()
        if not total_count:
            total_count = len(elements_list)
        # returns a list of sources, but first element is the amount of sources without pagination
        return {"total": total_count, "elements": [to_dict(elm) for elm in elements_list]}

    @manage_session
    def update_resource(self, resource_orm_class, pk, *, raise_if_bad_field=False, session=None, **kwargs):
        """
        Update a resource. Valid for resources without foreign keys
        :param resource_orm_class: ORM class related to the resource
        :param pk: Primary key of the existing resource
        :param raise_if_bad_field: True if you want to raise an exception when a non-existent field is specified for update
        :param session: Session to be used to execute the query
        :param kwargs: Keywords arguments, each one with name and new value of every field to update
        :return: True if everything goes well
        :raise ValueError if some
        """
        element = session.query(resource_orm_class).get(pk)
        if element is None:
            raise ValueError(f"No record in table '{resource_orm_class.__tablename__}' with pk='{pk}'")
        for field, new_value in kwargs.items():
            if not hasattr(element, field):
                if raise_if_bad_field:
                    raise ValueError(f"Table '{resource_orm_class.__tablename__}' has no '{field}' column")
                # fails silently by default
                continue
            setattr(element, field, new_value)
        # nothing else is needed because the execution of session.commit() is made out of this method
        return True

    @contextmanager
    def session_scope(self, engine_name='default', schema_name=None):
        """Provide a transactional scope around a series of operations."""
        engine = self.engines.get(engine_name)
        if engine is None:
            raise ValueError(f"No engine with name {engine_name}")
        if schema_name:
            connection = engine.connect().execution_options(
                schema_translate_map={None: schema_name}
            )
            session = self.Session(bind=connection)
        else:
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
