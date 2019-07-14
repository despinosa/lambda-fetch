"""Simple AWS lambda function for fetching data from a couple of database backends

The relational version assumes the following table exists:

    ```tweet.sql
    CREATE TABLE tweet (
        id SERIAL PRIMARY KEY,
        content VARCHAR(140) NOT NULL
    );
    ```

"""


import os

import sqlalchemy as sa

CONN_STRING = os.getenv('CONN_STRING')

ENGINE = sa.create_engine(CONN_STRING, max_overflow=5000)
SESSION_FACTORY = sa.sessionmaker(bind=ENGINE)


def handler(event, context):
    raise NotImplementedError
