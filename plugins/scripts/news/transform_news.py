from __future__ import annotations

import pandas as pd
from transformers import pipeline


def transform_news_bronze(
    df: pd.DataFrame,
    topic: str,
    job_log_id: int,
) -> pd.DataFrame:
    df["source"] = df["source"].apply(lambda x: x["name"])
    df["publishedAt"] = pd.to_datetime(df["publishedAt"])
    df["job_log_id"] = job_log_id
    df["topic"] = topic
    df.drop(columns=["urlToImage"], inplace=True)
    df.rename(columns={"publishedAt": "published_at"}, inplace=True)

    return df


MIN_WORD_COUNT_CONTENT = 10


def transform_news_silver(df):
    # Basic Transformations
    df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce")
    df["published_date"] = df["published_at"].dt.date.astype("datetime64[s]")
    df["year"] = df["published_date"].dt.year
    df["month"] = df["published_date"].dt.month
    df["day"] = df["published_date"].dt.day

    df = df[df["content"].apply(lambda x: len(x.split(" ")) >= MIN_WORD_COUNT_CONTENT)]
    df = df[df["title"].apply(lambda x: x != "[Removed]")]

    # Sentiment Analysis
    sentiment_pipeline = pipeline(
        "sentiment-analysis",
        model="nlptown/bert-base-multilingual-uncased-sentiment",
        tokenizer="mrm8488/distilroberta-finetuned-financial-news-sentiment-analysis",
    )

    def get_sentiment(text):
        result = sentiment_pipeline(text)
        return {"label": result[0]["label"], "score": result[0]["score"]}

    df["title_sentiment"] = df["title"].apply(lambda x: get_sentiment(x).get("label"))
    df["title_score"] = df["title"].apply(lambda x: get_sentiment(x).get("score"))

    df["content_sentiment"] = df["content"].apply(
        lambda x: get_sentiment(x).get("label"),
    )
    df["content_score"] = df["content"].apply(lambda x: get_sentiment(x).get("score"))

    # Drop duplicates
    df.drop_duplicates(inplace=True)
    return df


def transform_news_gold(df):
    return df
