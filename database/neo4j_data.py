from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable

import pandas as pd


class Neo4jDataLoader:
    _connect_params: dict
    _driver = None

    def __init__(self, **kwargs):
        self._connect_params = kwargs

    def __enter__(self):
        try:
            self._driver = GraphDatabase.driver(uri=self._connect_params['uri'],
                                               auth=(self._connect_params['user'], self._connect_params['password']))
            return self
        except ServiceUnavailable as e:
            print(e)
            raise

    def populate(self, df: pd.DataFrame):
        with self._driver.session(database="neo4j") as session:
            def trans_func(tx, **kwargs):
                query = (
                    "CREATE (h:Headline { author: $author, publish_date: $date, src: $src, content: $content }) "
                    "CREATE (s:Sentiment { sentiment: $sentiment }) "
                    "CREATE (h)-[:HAS_SENTIMENT]->(s) "
                )

                tx.run(query, **kwargs)

            for i in range(len(df)):
                row = df.iloc[i]
                session.execute_write(trans_func,
                                      author=row['author'],
                                      date=str(row['publish_date']),
                                      src=row['src'],
                                      content=row['content'],
                                      sentiment=bool(row['sentiment']))

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._driver is not None:
            self._driver.close()


# """Neo4j example code"""

# class App:
#     def __init__(self, uri, user, password):
#         self.driver = GraphDatabase.driver(uri, auth=(user, password))
#
#     def close(self):
#         # Don't forget to close the driver connection when you are finished with it
#         self.driver.close()
#
#     def create_friendship(self, person1_name, person2_name):
#         with self.driver.session(database="neo4j") as session:
#             # Write transactions allow the driver to handle retries and transient errors
#             result = session.execute_write(
#                 self._create_and_return_friendship, person1_name, person2_name)
#             for row in result:
#                 print("Created friendship between: {p1}, {p2}".format(p1=row['p1'], p2=row['p2']))
#
#     @staticmethod
#     def _create_and_return_friendship(tx, person1_name, person2_name):
#         # To learn more about the Cypher syntax, see https://neo4j.com/docs/cypher-manual/current/
#         # The Reference Card is also a good resource for keywords https://neo4j.com/docs/cypher-refcard/current/
#         query = (
#             "CREATE (p1:Person { name: $person1_name }) "
#             "CREATE (p2:Person { name: $person2_name }) "
#             "CREATE (p1)-[:KNOWS]->(p2) "
#             "RETURN p1, p2"
#         )
#         result = tx.run(query, person1_name=person1_name, person2_name=person2_name)
#         try:
#             return [{"p1": row["p1"]["name"], "p2": row["p2"]["name"]}
#                     for row in result]
#         # Capture any errors along with the query and data for traceability
#         except ServiceUnavailable as exception:
#             logging.error("{query} raised an error: \n {exception}".format(
#                 query=query, exception=exception))
#             raise
#
#     def find_person(self, person_name):
#         with self.driver.session(database="neo4j") as session:
#             result = session.execute_read(self._find_and_return_person, person_name)
#             for row in result:
#                 print("Found person: {row}".format(row=row))
#
#     @staticmethod
#     def _find_and_return_person(tx, person_name):
#         query = (
#             "MATCH (p:Person) "
#             "WHERE p.name = $person_name "
#             "RETURN p.name AS name"
#         )
#         result = tx.run(query, person_name=person_name)
#         return [row["name"] for row in result]
