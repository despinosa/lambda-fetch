"""Simple AWS lambda function for fetching data from a couple of database backends

The relational version assumes the following table exists:

    ```tweet.sql
    CREATE TABLE tweet (
        id SERIAL PRIMARY KEY,
        content VARCHAR(140) NOT NULL
    );
    ```

"""


import logging
import os

import sqlalchemy as sa


CONN_STRING = os.getenv('CONN_STRING')

ENGINE = sa.create_engine(CONN_STRING, max_overflow=5000)

META = sa.MetaData(ENGINE, reflect=True)
Tweet = META.tables(['tweet'])

SESSION_FACTORY = sa.sessionmaker(bind=ENGINE)


def fetch_from_relational(id: int) -> str:
    session = sa.scoped_session(SESSION_FACTORY)
    query = sa.select([Tweet.c.content]).where(Tweet.c.id == id)
    logging.debug('executing select: %s', query)
    content = session.execute(query)
    logging.debug('returning: %s', content)
    return content


def handler(event, context):
    if event['backend'] == 'relational':
        return fetch_from_relational(event['id'])
    else:
        raise NotImplementedError