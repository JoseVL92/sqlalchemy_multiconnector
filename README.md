# sqlalchemy-multiconnector  
Python SQLAlchemy connector for SQL databases (sqlite, postgresql and mysql).  
Easy resources CRUD management.  
Supports multi-tenancy with multiple databases and/or multiple schemas.

## Example of use
```python
from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, Text  
from sqlalchemy.orm import relationship  
from sqlalchemy_multiconnector import BASE, SQLConnector, manage_session  
  
  
DB_TYPE = 'postgresql'  # it also accepts 'mysql' and 'sqlite'
DB_HOST = 'hostname'  
DB_PORT = 5432
DB_NAME = 'test'
DB_USER = 'username'  
DB_PASSWD = 'password'  
  
  
class User(BASE):
    __tablename__ = 'users'  
    id = Column(Integer, autoincrement=True, primary_key=True)  
    username = Column(String)  
    active = Column(Boolean, default=True)  
    articles = relationship('Article', back_populates='user')  
  
  
class Article(BASE):  
    __tablename__ = 'articles'  
    id = Column(Integer, autoincrement=True, primary_key=True)  
    title = Column(String)  
    description = Column(Text, nullable=True)  
    user_id = Column(Integer, ForeignKey(User.id))  
    user = relationship('User', back_populates='articles')

# If some schema should be used, you can specify it on object creation
db_connection = SQLConnector(DB_TYPE, DB_HOST, DB_NAME, DB_PORT,  
                             db_schemas=['schema1', 'schema2'],
                             db_user=DB_USER,
                             db_passwd=DB_PASSWD)

# or with 'schemas' property
db_connection.schemas = ['schema1', 'schema2']

# or in 'create_tables' method
db_connection.create_tables(schemas=['schema1', 'schema2'])

# add a user in schema1
user_id = db_connection.create_resource(User, {'username': 'Usuario', 'is_active': False}, return_id=True, schema_name='schema1')

# add an article related to this user
with db_connection.session_scope(schema_name = 'schema1') as session:
	user_owner = session.query(User).get(user_id)
	article_id = db_connection.create_resource(Article, {'title': 'Hi', 'user': user_owner}, session=session)

# update article title
db_connection.update_resource(Article, pk=article_id, updated_fields: {'title': 'Hi again!'})

# get and delete a specific resource and list all resources are also available
```

