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


class RelationalConfig(object):
    conn_string = os.getenv('CONN_STRING')

    engine = sa.create_engine(conn_string, max_overflow=5000)

    meta = sa.MetaData(engine, reflect=True)
    Tweet = meta.tables(['tweet'])

    session_factory = sa.sessionmaker(bind=engine)


def fetch_from_relational(id: int) -> str:
    session = sa.scoped_session(RelationalConfig.session_factory)
    query = sa.select(
        [RelationalConfig.Tweet.c.content]
    ).where(RelationalConfig.Tweet.c.id == id)
    logging.debug('executing select: %s', query)
    content = session.execute(query)
    logging.debug('returning: %s', content)
    return content


def fetch_from_dynamo(id: int) -> str:
    raise NotImplementedError


def handler(event, context) -> str:
    id_ = int(event['id'])
    if event['backend'] == 'relational':
        return fetch_from_relational(id_)
    elif event['backend'] == 'dynamo':
        return fetch_from_dynamo(id_)
    else:
        raise NotImplementedError