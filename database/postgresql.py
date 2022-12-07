from typing import Any, Optional, Union

import psycopg2 as postgres

import pandas as pd


_tables = [
    """CREATE TABLE Headline (
        id SERIAL PRIMARY KEY,
        author VARCHAR(255),
        publish_date TIMESTAMP,
        src VARCHAR(255),
        content TEXT
    );""",
    """CREATE TABLE Sentiment (
        id SERIAL PRIMARY KEY,
        headline_id INT NOT NULL,
        sentiment BOOL,
        FOREIGN KEY (headline_id)
            REFERENCES Headline(id)
            ON DELETE CASCADE
    );"""
]

_drop_tables = [
    'DROP TABLE IF EXISTS Sentiment;',
    'DROP TABLE IF EXISTS Headline;'
]


class PostgresDataLoader:
    _connect_params: dict
    _cnx = None

    def __init__(self, **kwargs):
        self._connect_params = kwargs

    def __enter__(self):
        try:
            self._cnx = postgres.connect(**self._connect_params)
            return self
        except postgres.Error as e:
            print(e)
            raise

    def create(self, drop_first=False):
        cursor = self._cnx.cursor()

        if drop_first:
            for query in _drop_tables:
                cursor.execute(query)

                print(f'Executed {query}')

        cursor.close()
        self._cnx.commit()

        cursor = self._cnx.cursor()
        for query in _tables:
            cursor.execute(query)

            print(f'Executed {query}')
        cursor.close()
        self._cnx.commit()

    def populate(self, df: pd.DataFrame):
        cursor = self._cnx.cursor()

        for i in range(len(df)):
            row = df.iloc[i]

            insert = """
            INSERT INTO Headline (author, publish_date, src, content)
            VALUES (%s, %s, %s, %s)
            RETURNING id;
            """
            cursor.execute(insert, [row['author'], row['publish_date'], row['src'], row['content']])

            headline_id = next(iter(cursor))[0]

            insert_sentiment = """
            INSERT INTO Sentiment (sentiment, headline_id)
            VALUES(%s, %s);
            """
            cursor.execute(insert_sentiment, [bool(row['sentiment']), headline_id])

        cursor.close()
        self._cnx.commit()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._cnx.close()
