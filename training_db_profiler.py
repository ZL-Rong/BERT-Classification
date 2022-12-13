from training_model_db import mysql_dataset, postgres_dataset, neo4j_dataset

from timeit import timeit

from random import randint

if __name__ == '__main__':
    with mysql_dataset() as database:
        t = timeit(lambda: database.__getitem__(randint(1, 10000)), number=1000)
        print('MySQL', t, 'ms')

    with postgres_dataset() as database:
        t = timeit(lambda: database.__getitem__(randint(1, 10000)), number=1000)
        print('PostgreSQL', t, 'ms')

    with neo4j_dataset() as database:
        t = timeit(lambda: database.__getitem__(randint(1, 10000)), number=100)
        print('Neo4j', t * 10, 'ms')