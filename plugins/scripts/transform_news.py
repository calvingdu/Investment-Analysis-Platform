from __future__ import annotations

import pandas as pd


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
