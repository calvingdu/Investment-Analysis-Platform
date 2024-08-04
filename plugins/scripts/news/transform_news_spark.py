from __future__ import annotations

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import dayofmonth
from pyspark.sql.functions import month
from pyspark.sql.functions import to_date
from pyspark.sql.functions import udf
from pyspark.sql.functions import year
from pyspark.sql.types import DateType
from pyspark.sql.types import FloatType
from pyspark.sql.types import StringType
from transformers import pipeline


MIN_WORD_COUNT_CONTENT = 10


def transform_news_silver_spark(df):
    spark = SparkSession.builder.appName("DataFrameExample").getOrCreate()

    df = df.withColumn("published_at", to_date(col("published_at"), "yyyy-MM-dd"))
    df = df.withColumn("published_date", col("published_at").cast(DateType()))
    df = df.withColumn("year", year(col("published_date")))
    df = df.withColumn("month", month(col("published_date")))
    df = df.withColumn("day", dayofmonth(col("published_date")))

    def filter_content(content):
        return len(content.split(" ")) >= MIN_WORD_COUNT_CONTENT

    filter_content_udf = udf(filter_content)
    df = df.filter(filter_content_udf(col("content")))
    df = df.filter(col("title") != "[Removed]")

    # Initialize sentiment analysis pipeline
    sentiment_pipeline = pipeline(
        "sentiment-analysis",
        model="nlptown/bert-base-multilingual-uncased-sentiment",
        tokenizer="mrm8488/distilroberta-finetuned-financial-news-sentiment-analysis",
    )

    def get_sentiment_label(text):
        result = sentiment_pipeline(text)
        return result[0]["label"]

    def get_sentiment_score(text):
        result = sentiment_pipeline(text)
        return float(result[0]["score"])

    sentiment_label_udf = udf(get_sentiment_label, StringType())
    sentiment_score_udf = udf(get_sentiment_score, FloatType())

    # Apply sentiment analysis UDFs
    df = df.withColumn("title_sentiment", sentiment_label_udf(col("title")))
    df = df.withColumn("title_score", sentiment_score_udf(col("title")))
    df = df.withColumn("content_sentiment", sentiment_label_udf(col("content")))
    df = df.withColumn("content_score", sentiment_score_udf(col("content")))

    df = df.dropDuplicates()

    return df
