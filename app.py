from transformers import AutoTokenizer
from transformers import BertTokenizer, BertModel
from transformers import pipeline
import numpy as np
import config
import torch
import flask
from flask import Flask,render_template,url_for,request,flash,session
import torch.nn as nn
import transformers
import logging
import sys

app = Flask(__name__)
app.secret_key= '\xa8\x08y\x87\x9bL(\xcf\\@\xbfX'

DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")

@app.route('/')
def homepage():
    return render_template('index.html')

def sentence_prediction(sentence, model):
    tokenizer = AutoTokenizer.from_pretrained("bert-base-cased")
    max_len = 128
    article = str(sentence)
    # article = " ".join(article.split())
    # inputs = tokenizer.encode_plus(
    #     article,
    #     max_length=max_len,
    # )
    inputs = tokenizer(article, return_tensors="pt")
    with torch.no_grad():
        input_ids = inputs['input_ids'].to('cpu', dtype=torch.long)
        attention_mask = inputs['attention_mask'].to('cpu', dtype=torch.long)
        token_type_ids = inputs['token_type_ids'].to('cpu', dtype=torch.long)
        output = model(input_ids, attention_mask, token_type_ids)
        final_output = torch.sigmoid(output[1]).cpu().detach().numpy()
        outputs = final_output


    print(outputs.shape)
    return outputs[0][0]


@app.route("/predict", methods=['POST'])
def predict():
    sentence = request.form.get("sentence")
    positive_prediction = sentence_prediction(sentence, model=MODEL)
    negative_prediction = 1 - positive_prediction
    response = {}
    response = {
        'positive': positive_prediction,
        'negative': negative_prediction,
        'sentence': str(sentence)
    }
    if not sentence:
        flash("Form is blank!")
        return render_template('index.html')
    else:
        return render_template("result.html", sentence=response['sentence'],
                    positive=response['positive'], negative=response['negative'])



if __name__ == "__main__":
    MODEL = BertModel.from_pretrained("./my_model/")
    MODEL.to(DEVICE)
    MODEL.eval()
    app.run(debug=True, host="127.0.0.1")
