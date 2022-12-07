from transformers import AutoTokenizer

TRAIN_BATCH_SIZE = 8
VALID_BATCH_SIZE = 4
EPOCHS = 10
BERT_PATH = "./my_model/"
MODEL_PATH = "./my_model/pytorch_model.bin"
TRAINING_FILE = "./train.csv"
TOKENIZER = AutoTokenizer.from_pretrained("bert-base-cased"
    # BERT_PATH,
    # do_lower_case=True
)
