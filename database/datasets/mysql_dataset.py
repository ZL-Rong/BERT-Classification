from typing import Any, Callable, Optional, Union

import mysql.connector as sql

from torch.utils.data import Dataset


class MysqlDataset(Dataset[Any]):
    _connect_params: dict
    _cnx: sql.MySQLConnection
    _query: str
    _len: Optional[int] = None
    _len_query: Optional[str] = None
    _tokenizer: Optional[Callable] = None

    def __init__(self, query: str, length: Union[int, str], tokenizer: Optional[Callable] = None, **kwargs):
        self._query = query
        if isinstance(length, int):
            self._len = length
        else:
            self._len_query = length
        self._tokenizer = tokenizer
        self._connect_params = kwargs

    def __enter__(self):
        try:
            self._cnx = sql.connect(**self._connect_params)
            return self
        except sql.Error as e:
            print(e)
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._cnx.close()

    def __getitem__(self, item) -> Any:
        cursor = self._cnx.cursor()
        cursor.execute(self._query, (item + 1,))

        it = iter(cursor)
        rv = next(it, None)
        if rv is not None:
            rv = dict(zip(cursor.column_names, rv))
        if rv is not None and self._tokenizer is not None:
            rv |= dict(self._tokenizer(rv['text'], padding="max_length", truncation=True))

        while next(it, None) is not None:
            pass

        cursor.close()
        return rv

    def __len__(self):
        if self._len is not None:
            return self._len
        elif self._len_query is not None:
            cursor = self._cnx.cursor()
            cursor.execute(self._len_query)

            it = iter(cursor)
            rv = next(it, None)
            if rv is not None:
                rv = int(rv[0])

            while next(it, None) is not None:
                pass

            cursor.close()
            return rv

        return None
