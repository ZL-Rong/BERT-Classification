from typing import Any, Optional, Union

import mysql.connector as sql

import pandas as pd


_tables = [
    """CREATE TABLE Headline (
        id INT PRIMARY KEY AUTO_INCREMENT,
        author VARCHAR(255),
        publish_date DATETIME,
        src VARCHAR(255),
        content TEXT
    );""",
    """CREATE TABLE Sentiment (
        id INT PRIMARY KEY AUTO_INCREMENT,
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


class MysqlDataLoader:
    _connect_params: dict
    _cnx: sql.MySQLConnection

    def __init__(self, **kwargs):
        self._connect_params = kwargs

    def __enter__(self):
        try:
            self._cnx = sql.connect(**self._connect_params)
            return self
        except sql.Error as e:
            print(e)
            raise

    def create(self, drop_first=False):
        cursor = self._cnx.cursor()

        if drop_first:
            for query in _drop_tables:
                cursor.execute(query)
                cursor.fetchall()

                print(f'Executed {query}')

        cursor.close()
        self._cnx.commit()

        cursor = self._cnx.cursor()
        for query in _tables:
            cursor.execute(query)
            cursor.fetchall()

            print(f'Executed {query}')
        cursor.close()
        self._cnx.commit()

    def populate(self, df: pd.DataFrame):
        cursor = self._cnx.cursor()

        for i in range(len(df)):
            row = df.iloc[i]

            insert = """
            INSERT INTO Headline (author, publish_date, src, content)
            VALUES (%s, %s, %s, %s);
            """
            cursor.execute(insert, [row['author'], row['publish_date'], row['src'], row['content']])

            insert_sentiment = """
            INSERT INTO Sentiment (sentiment, headline_id)
            VALUES(%s, LAST_INSERT_ID());
            """
            cursor.execute(insert_sentiment, [bool(row['sentiment'])])

        cursor.close()
        self._cnx.commit()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._cnx.close()
