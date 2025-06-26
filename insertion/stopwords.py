#! /usr/bin/env python3

import os
import pandas as pd

from nltk import download, pos_tag, help
from nltk.corpus import stopwords
from sqlalchemy import create_engine

from pyspark.sql import SparkSession

def load_stopwords():
    success = False
    stop_words = None

    while not success:
        try:
            stop_words =  stopwords.words('english')
            success = True
        except LookupError:
            print(f"Stopwords não encontradas dentro da biblioteca! Baixando...")
            download('stopwords')
    
    return stop_words

def get_tag_description(tag):
    try:
        import io, sys
        captured = io.StringIO()
        sys.stdout = captured
        help.upenn_tagset(tag)
        sys.stdout = sys.__stdout__

        description = captured.getvalue().strip().splitlines()[0].split(":")[1].strip().split(',')[0].strip()
        return description

    except Exception as e:
        return None


def main():

    download('tagsets_json')

    spark = SparkSession.builder.appName("Ydan stopwords insertion").getOrCreate()
    
    stopwords = load_stopwords()




    df_stopwords = pd.DataFrame(
        {
            "word": stopwords,
            "char_count": [1 for _ in stopwords],
            "grammar_class": [get_tag_description(x[1]) for x in pos_tag(stopwords)],
            "language": ["en" for _ in stopwords]
        }
    )

    df_stopwords.to_csv('./data/dictionary.csv', sep=',', index=False)


if __name__ == "__main__":
    main()