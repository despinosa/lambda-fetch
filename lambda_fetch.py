"""Simple AWS lambda function for fetching data from a couple of database backends

The relational version assumes the following table exists:

    ```tweet.sql
    CREATE TABLE tweet (
        id SERIAL PRIMARY KEY,
        content VARCHAR(140) NOT NULL
    );
    ```

The dynamo table would look like
```tweet.json
{
    "TableName": "tweet",
    "KeySchema": [
        {"AttrributeName": "id",
         "KeyType": "HASH"}
    ],
    "AttributeDefinitions" : [
        {"AttributeName": "id",
         "AttributeType": "N"},
        {"AttributeName": "content",
         "AttributeType": "S"}
    ],
    "ProvisionedThroughput": {
        "ReadCapacityUnits": 5,
        "WriteCapacityUnits": 5
    },
    "Tags": [
      {"Key": "Owner",
       "Value": "despinosa"}
   ]
}
"""


import logging
import os

import boto3 as b3
import botocore.exceptions as b3_err
import sqlalchemy as sa
import sqlalchemy.exc as sa_err


class RelationalConfig(object):
    engine = sa.create_engine(
        os.getenv('SQL_CONN_STRING', ''),
        max_overflow=5
    )

    meta = sa.MetaData(engine, reflect=True)
    Tweet = meta.tables(['tweet'])

    session_factory = sa.sessionmaker(bind=engine)


class DynamoConfig(object):
    database = b3.resource(
        'dynamodb',
        region_name=os.getenv('AWS_REGION_NAME', ''),
        endpoint_url=os.getenv('ENDPOINT_URL', '')
    )
    Tweet = database.Table('tweet')


def fetch_from_relational(id_: int) -> str:
    session = sa.scoped_session(RelationalConfig.session_factory)
    query = sa.select(
        [RelationalConfig.Tweet.c.content]
    ).where(RelationalConfig.Tweet.c.id == id_)
    logging.debug('executing select: %s', query)
    content = session.execute(query)
    logging.debug('returning: %s', content)
    return content


def fetch_from_dynamo(id_: int) -> str:
    response = DynamoConfig.Tweet.get_item(Key={'id' == id_})
    logging.debug('retrieved response: %s', response)
    return response.get('Item', {})['content']  # avoid KeyError


def handler(event, context) -> str:
    id_ = int(event['id'])
    if event['backend'] == 'relational':
        try:
            return fetch_from_relational(id_)
        except sa_err.DBAPIError as error:
            raise RuntimeError('an error ocurred') from error
    elif event['backend'] == 'dynamo':
        try:
            return fetch_from_dynamo(id_)
        except b3_err.ClientError as error:
            raise RuntimeError('an error ocurred') from error
    raise NotImplementedError
