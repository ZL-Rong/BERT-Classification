from typing import Any, Callable, Optional, Union

from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable

from torch.utils.data import Dataset


class Neo4jDataset(Dataset[Any]):
    _connect_params: dict
    _driver = None
    _query: str
    _len: Optional[int] = None
    _len_query: Optional[str] = None
    _tokenizer: Optional[Callable] = None

    def __init__(self, query: str, length: int, tokenizer: Optional[Callable] = None, **kwargs):
        self._query = query
        self._len = length
        self._tokenizer = tokenizer
        self._connect_params = kwargs

    def __enter__(self):
        try:
            self._driver = GraphDatabase.driver(uri=self._connect_params['uri'],
                                                auth=(self._connect_params['user'], self._connect_params['password']))
            return self
        except ServiceUnavailable as e:
            print(e)
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._driver is not None:
            self._driver.close()

    def __getitem__(self, item) -> Any:
        with self._driver.session(database="neo4j") as session:
            def trans_func(tx, **kwargs):
                return [{'text': row['text'], 'label': row['label']} for row in tx.run(self._query, **kwargs)]

            result = session.execute_read(trans_func, id=item * 2)

            it = iter(result)
            rv = next(it, None)
            if rv is not None and self._tokenizer is not None:
                rv |= dict(self._tokenizer(rv['text'], padding="max_length", truncation=True))

            return rv

    def __len__(self):
        return self._len
