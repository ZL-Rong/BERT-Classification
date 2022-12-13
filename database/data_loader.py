import pandas as pd

from credential_loader import load_credentials
from mysql_data import MysqlDataLoader
from postgresql import PostgresDataLoader
from neo4j_data import Neo4jDataLoader

from random import randint

import os


def load_mysql():
    with MysqlDataLoader(**load_credentials('mysql')) as mysql:
        mysql.create(drop_first=True)

        for file in [r'..\train.csv', r'..\test.csv', r'..\newdatasetwithcoviddata.csv']:
            df = pd.read_csv(file).dropna().rename(columns={'text': 'content', 'label': 'sentiment'})
            df['src'] = os.path.basename(file)
            df['author'] = [t[20:40].replace(' ', '') + str(randint(10, 10000)) for t in df['content']]
            df['publish_date'] = [pd.Timestamp.now() - pd.Timedelta(t + randint(1, 300), unit='d') for t in df['author'].str.len()]

            mysql.populate(df)
            print(f'Populated {file}')

def load_postgres():
    with PostgresDataLoader(**load_credentials('postgresql')) as postgres:
        postgres.create(drop_first=True)

        for file in [r'..\train.csv', r'..\test.csv', r'..\newdatasetwithcoviddata.csv']:
            df = pd.read_csv(file).dropna().rename(columns={'text': 'content', 'label': 'sentiment'})
            df['src'] = os.path.basename(file)
            df['author'] = [t[20:40].replace(' ', '') + str(randint(10, 10000)) for t in df['content']]
            df['publish_date'] = [pd.Timestamp.now() - pd.Timedelta(t + randint(1, 300), unit='d') for t in df['author'].str.len()]

            postgres.populate(df)
            print(f'Populated {file}')


def load_neo4j():
    with Neo4jDataLoader(**load_credentials('neo4j')) as neo:
        for file in [r'..\train.csv', r'..\test.csv', r'..\newdatasetwithcoviddata.csv']:
            df = pd.read_csv(file).dropna().rename(columns={'text': 'content', 'label': 'sentiment'})
            df['src'] = os.path.basename(file)
            df['author'] = [t[20:40].replace(' ', '') + str(randint(10, 10000)) for t in df['content']]
            df['publish_date'] = [pd.Timestamp.now() - pd.Timedelta(t + randint(1, 300), unit='d') for t in df['author'].str.len()]

            neo.populate(df)
            print(f'Populated {file}')


if __name__ == '__main__':
    load_neo4j()