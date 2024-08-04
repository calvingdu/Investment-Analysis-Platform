from __future__ import annotations

import datetime
import os

import fmpsdk
import pandas as pd

from airflow.models import BaseOperator

STOCK_API_KEY = os.getenv("STOCK_API_KEY")


class StockCompanyProfilesAPIToDataframeOperator(BaseOperator):
    def __init__(
        self,
        symbols: str,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.symbols = symbols

    def execute(self, **context) -> pd.DataFrame:
        profiles = []
        for s in self.symbols:
            try:
                data = fmpsdk.company_profile(apikey=STOCK_API_KEY, symbol=s)
                if data:
                    profiles.extend(data)
                else:
                    print(f"No data found for symbol: {s}")
            except Exception as e:
                print(f"Error fetching data for symbol: {s}. Error: {e}")

        df = pd.DataFrame.from_dict(profiles)
        return df


class StockQuoteAPIToDataframeOperator(BaseOperator):
    def __init__(
        self,
        symbol: str,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.symbol = symbol

    def execute(self, **context) -> pd.DataFrame:
        data = fmpsdk.quote(apikey=STOCK_API_KEY, symbol=self.symbol)
        df = pd.DataFrame(data)
        return df


class StockHistoricalDividendsAPIToDataframeOperator(BaseOperator):
    def __init__(
        self,
        symbol: str,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.symbol = symbol

    def execute(self, **context) -> pd.DataFrame:
        data = fmpsdk.historical_stock_dividend(
            apikey=STOCK_API_KEY,
            symbol=self.symbol,
        )
        df = pd.DataFrame(data)
        return df


class StockHistoricalPricesDataframeOperator(BaseOperator):
    def __init__(
        self,
        symbol: str,
        from_date: str = datetime.datetime.now() - datetime.timedelta(days=365),
        to_date: str = datetime.datetime.now(),
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.symbol = symbol
        self.from_date = from_date
        self.to_date = to_date

    def execute(self, **context) -> pd.DataFrame:
        data = fmpsdk.historical_price_full(
            apikey=STOCK_API_KEY,
            symbol=self.symbol,
            from_date=self.from_date,
            to_date=self.to_date,
        )
        df = pd.DataFrame(data)
        return df
