from transformers import AutoTokenizer
from transformers import AutoModelForSequenceClassification
import numpy as np
import evaluate
from transformers import TrainingArguments, Trainer

from torch.utils.data import random_split

from database.credential_loader import load_credentials
from database.datasets.mysql_dataset import MysqlDataset
from database.datasets.postgres_dataset import PostgresqlDataset
from database.datasets.neo4j_dataset import Neo4jDataset


def mysql_dataset():
    query = """
    SELECT content AS text, sentiment AS label
        FROM Headline AS H
        JOIN Sentiment AS S ON H.id = S.headline_id
        WHERE H.id=%s
        LIMIT 1
    """

    tokenizer = AutoTokenizer.from_pretrained("bert-base-cased")
    return MysqlDataset(query, length=4096, tokenizer=tokenizer,
                        **load_credentials('mysql', r'.\database\credentials.json'))


def postgres_dataset():
    query = """
        SELECT content AS text, sentiment AS label
            FROM Headline AS H
            JOIN Sentiment AS S ON H.id = S.headline_id
            WHERE H.id=%s
            LIMIT 1
        """

    tokenizer = AutoTokenizer.from_pretrained("bert-base-cased")
    return PostgresqlDataset(query, length=4096, tokenizer=tokenizer,
                             **load_credentials('postgresql', r'.\database\credentials.json'))


def neo4j_dataset():
    query = (
        "MATCH (h:Headline)-->(s) "
        "WHERE id(h) = $id "
        "RETURN h.content AS text, s.sentiment AS label "
        "LIMIT 1 "
    )

    tokenizer = AutoTokenizer.from_pretrained("bert-base-cased")
    return Neo4jDataset(query, length=4096, tokenizer=tokenizer,
                        **load_credentials('neo4j', r'.\database\credentials.json'))


if __name__ == '__main__':
    with neo4j_dataset() as database:
        # load the datasets
        train_size = int(len(database) * 0.8)
        train, test = random_split(database, [train_size, len(database) - train_size])

        model = AutoModelForSequenceClassification.from_pretrained("bert-base-cased", num_labels=5)

        metric = evaluate.load("accuracy")

        def compute_metrics(eval_pred):
            logits, labels = eval_pred
            predictions = np.argmax(logits, axis=-1)
            return metric.compute(predictions=predictions, references=labels)

        training_args = TrainingArguments(output_dir="test_trainer", evaluation_strategy="epoch")

        trainer = Trainer(
            model=model,
            args=training_args,
            # train_dataset=small_train_dataset,
            # eval_dataset=small_eval_dataset,
            train_dataset=train,
            eval_dataset=test,
            compute_metrics=compute_metrics,
        )

        trainer.train()
        trainer.save_model("./my_model")