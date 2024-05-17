from __future__ import annotations

import datetime
import os

import pandas as pd
import requests

from airflow.models import BaseOperator

NEWS_API_KEY = os.getenv("NEWS_API_KEY")


def get_news(
    query: str,
    endpoint: str,
    from_date: str,
    to_date: str,
    sort_by: str,
    page_size: int,
    page_number: int,
    **kwargs,
) -> dict:
    if endpoint not in ["top-headlines", "everything"]:
        raise ValueError('Endpoint must be either "top-headlines" or "everything"')

    if sort_by not in ["relevancy", "popularity", "publishedAt"]:
        raise ValueError(
            'Sort by must be either "relevancy", "popularity", or "publishedAt"',
        )

    if from_date is None:
        from_date = datetime.now().strftime("%Y-%m-%d")

    url = (
        "https://newsapi.org/v2/everything?"
        f"q={query}&"
        f"from={from_date}&"
        f"to={to_date}&"
        f"sortBy={sort_by}&"
        f"pageSize={page_size}&"
        f"page={page_number}&"
        f"language=en&"
        f"apiKey={NEWS_API_KEY}"
    )

    response = requests.get(url)
    return response.json()


class NewsAPIToDataframeOperator(BaseOperator):
    def __init__(
        self,
        news_topic: str,
        endpoint: str,
        from_date: str,
        to_date: str,
        sort_by: str,
        page_size: int,
        page_number: int,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.news_topic = news_topic
        self.endpoint = endpoint
        self.from_date = from_date
        self.to_date = to_date
        self.sort_by = sort_by
        self.page_size = page_size
        self.page_number = page_number

    def execute(self, **context) -> pd.DataFrame:
        news_result = get_news(
            self.news_topic,
            self.endpoint,
            self.from_date,
            self.to_date,
            self.sort_by,
            self.page_size,
            self.page_number,
        )
        articles = news_result["articles"]
        return pd.DataFrame(articles)
