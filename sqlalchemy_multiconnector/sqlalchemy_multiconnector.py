from datetime import datetime

from contextlib import contextmanager
from functools import wraps

from sqlalchemy import create_engine
from sqlalchemy.engine.row import Row
from sqlalchemy.exc import InvalidRequestError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, class_mapper, Session

BASE = declarative_base()


def decompose_fields(fields: list):
    """
    Auxiliary func to check if 'fields' has a relationship expressed as <rel_name.rel_property>
    :return Tuple (A, B) where A is the list of fields divided into possible relations and its subproperties,
            and B is a boolean expressing if there is at least one relation in this fields
            Ex: ([('name',), ('base')], False)           ---> no relation
                ([('source', 'title'), ('name',)], True) ---> there is a relation with 'source'
    """
    if not fields:
        return [], False
    # Check if there are any '*.*' pattern in any field,
    # which indicates we need to retrieve some relationship property
    splitted_fields = [f.split('.') for f in fields]
    are_relations = [len(sf) > 1 for sf in splitted_fields]
    return splitted_fields, any(are_relations)


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

    def create_tables(self, schemas: [] = None):
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

    def _create_schemas(self, schemas: [] = None):
        schemas = schemas or self.schemas
        if schemas is None:
            return
        if isinstance(schemas, str):
            schemas = [schemas]
        for engine_name, _ in self.engines.items():
            for sc in schemas:
                self.execute_query("CREATE SCHEMA IF NOT EXISTS " + sc, engine_name)

    def _dynamic_relations(self, resource_orm_class: BASE, rel_deep_list: list):
        chained = getattr(resource_orm_class, rel_deep_list[0])
        if len(rel_deep_list) > 1:
            return self._dynamic_relations(chained, rel_deep_list[1:])
        return chained

    def execute_query(self, query: str, engine_name: str = None):
        """Execute a raw query on database 'engine_name'.
        If any schema will be used, it must be specified in the sql statement"""
        if engine_name is None:
            engine_name = 'default'
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
    def compose_filter_query(self,
                             resource_orm_class: BASE, resource_query_binding_class, filter_and_sort_dict: dict = None,
                             fields: list = None, limit: int = 1000, offset: int = 0, *, session: Session = None):
        """
        Same as 'list_resources' but only returns the total count and query itself, not evaluated
        :return: SQLAlchemy Query object
        """
        _, are_relations = decompose_fields(fields)

        if filter_and_sort_dict:
            query = resource_query_binding_class(session=session).evaluate_params(filter_and_sort_dict)
        else:
            query = session.query(resource_orm_class)

        if fields and not are_relations:
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

        return total_count, query

    @manage_session
    def create_resource(self, resource_orm_class: BASE, resource_fields: dict, *, return_id: bool = False,
                        session: Session = None, **kwargs):
        """
        Add a resource. Doesn't check for integrity errors. Valid for resources without foreign keys.
        :param resource_orm_class: ORM class related to the resource
        :param resource_fields: Dictionary with column names of the new object as keys and their respective values
        :param return_id: If it needs to commit this query to catch the new autocreated 'id' and returning it
        :param session: Session to be used to execute query
        :param kwargs: Additional keyword arguments for session (eg: db_name or schema_name)
        :return: True (or resource 'id' if return_id is True) if the operation succeeded
        """
        resource = resource_orm_class(**resource_fields)
        session.add(resource)
        if return_id:
            session.flush()
            session.refresh(resource)
            return resource.id
        return True

    @manage_session
    def delete_resource(self, resource_orm_class: BASE, pk, *, session: Session = None, **kwargs):
        """
        Deletes a resource
        :param resource_orm_class: ORM class related to the resource
        :param pk: Primary key value
        :param session: Session to be used to execute query
        :param kwargs: Additional keyword arguments for session (eg: db_name or schema_name)
        :return: True if the operation succeeded
        """
        resource = session.query(resource_orm_class).get(pk)
        if resource is not None:
            session.delete(resource)
        return True

    @manage_session
    def get_resource(self, resource_orm_class: BASE, pk, pk_fieldname: str = None, fields: list = None, *,
                     just_check_existence: bool = False, session: Session = None, **kwargs):
        """
        Get details about a specific resource.
        :param resource_orm_class: ORM class related to the resource
        :param pk: Primary key value
        :param pk_fieldname: Primary key column name.
        :param fields: Desired columns to be returned.
        :param just_check_existence: If this method is invoked just to check resource existence
        :param session: Session to be used to execute query
        :param kwargs: Additional keyword arguments for session (eg: db_name or schema_name)
        :return: A dictionary with the resource information
        :raise: ValueError if no resource with 'pk' primary key value is found
        """
        splitted_fields, are_relations = decompose_fields(fields)

        if not pk_fieldname or not fields or are_relations:
            resource = session.query(resource_orm_class).get(pk)
        else:
            # retrieving specific fields is a much more efficient way to query
            fields = [getattr(resource_orm_class, f) for f in fields]
            resource = session.query(*fields).filter(getattr(resource_orm_class, pk_fieldname) == pk).one_or_none()
        if just_check_existence:
            return resource is not None

        if resource is None:
            raise ValueError(f"Resource '{resource_orm_class.__tablename__}' with pk='{pk}' not found")

        if fields:
            return {'.'.join(sf): self._dynamic_relations(resource, sf) for sf in splitted_fields}
        return to_dict(resource)

    @manage_session
    def list_resources(self, resource_orm_class: BASE, resource_query_binding_class, filter_and_sort_dict: dict = None,
                       fields: list = None, limit: int = 1000, offset: int = 0, *, session: Session = None, **kwargs):
        """
        Get a list of resources that meet a set of parameters
        :param resource_orm_class: ORM class related to the resource
        :param resource_query_binding_class: QueryBinding-based class (from 'sqlalchemy-filterparams')
        :param filter_and_sort_dict: Dictionary of options specified by 'filterparams' library
        :param fields: Columns to be selected
        :param limit: Max number of rows fetched
        :param offset: Number of rows to skip before starting to return rows from the query
        :param session: Session to be used to execute the query
        :param kwargs: Additional keyword arguments for session (eg: db_name or schema_name)
        :return: A dictionary with shape {"total": total_count, "resources": [resources_list]}
        """
        if limit > 1000:
            raise ValueError("Limit out of bounds")
        total_count, query = self.compose_filter_query(resource_orm_class, resource_query_binding_class,
                                                       filter_and_sort_dict, fields, limit, offset, session=session)

        # if are_relations, returned query just ignored fields
        splitted_fields, are_relations = decompose_fields(fields)
        resources_list = query.all()
        if not total_count:
            total_count = len(resources_list)

        if fields:
            response = [
                {'.'.join(sf): self._dynamic_relations(resource, sf) for sf in splitted_fields} for resource in
                resources_list
            ]
        else:
            response = [to_dict(rsc) for rsc in resources_list]

        # returns a list of sources, but first element is the amount of sources without pagination
        return {"total": total_count, "resources": response}

    @manage_session
    def update_resource(self, resource_orm_class: BASE, pk, updated_fields: dict, *, raise_if_bad_field: bool = False,
                        session: Session = None, **kwargs):
        """
        Update a resource. Valid for resources without foreign keys
        :param resource_orm_class: ORM class related to the resource
        :param pk: Primary key of the existing resource
        :param updated_fields: Dictionary with column names of the updated object as keys and their respective new values
        :param raise_if_bad_field: True if you want to raise an exception when a non-existent field is specified for update
        :param session: Session to be used to execute the query
        :param kwargs: Additional keyword arguments for session (eg: db_name or schema_name)
        :return: True if everything goes well
        :raise ValueError if some
        """
        resource = session.query(resource_orm_class).get(pk)
        if resource is None:
            raise ValueError(f"No record in table '{resource_orm_class.__tablename__}' with pk='{pk}'")
        for field, new_value in updated_fields.items():
            if not hasattr(resource, field):
                if raise_if_bad_field:
                    raise ValueError(f"Table '{resource_orm_class.__tablename__}' has no '{field}' column")
                # fails silently by default
                continue
            setattr(resource, field, new_value)
        # nothing else is needed because the execution of session.commit() is made out of this method
        return True

    @contextmanager
    def session_scope(self, engine_name: str = None, schema_name: str = None):
        """Provide a transactional scope around a series of operations."""
        engine_name = engine_name or 'default'
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
