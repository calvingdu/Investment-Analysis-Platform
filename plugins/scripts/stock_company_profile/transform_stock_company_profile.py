from __future__ import annotations

import datetime

import pandas as pd


def transform_stock_company_profile_bronze(
    df: pd.DataFrame,
    job_log_id: int,
    date=datetime.datetime.now(),
):
    df.columns = map(str.lower, df.columns)

    df["job_log_id"] = job_log_id
    df["processed_date"] = pd.to_datetime(date.strftime("%Y-%m-%d"))

    return df


def transform_stock_company_profile_silver(df: pd.DataFrame):
    price_ranges = df["range"].apply(lambda x: x.split("-"))
    df["52_week_low"] = price_ranges.apply(lambda x: x[0])
    df["52_week_high"] = price_ranges.apply(lambda x: x[1])

    df["processed_year"] = df["processed_date"].apply(lambda x: x.year)
    df["processed_month"] = df["processed_date"].apply(lambda x: x.month)
    df["processed_day"] = df["processed_date"].apply(lambda x: x.day)

    df.drop(columns=["range"], inplace=True)
    df.drop(columns=["image", "defaultImage"], inplace=True)
    return df
