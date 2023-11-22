import datetime as dt
import logging
from io import StringIO
from typing import Dict, List

import pandas as pd
import requests
from requests.exceptions import HTTPError, ConnectionError
from flask import current_app
from tiingo import TiingoClient

from portfolio_builder import db, scheduler
from portfolio_builder.public.models import (
    Price, Security, WatchlistItem,
    SecurityMgr, PriceMgr, WatchlistItemMgr
)


ASSET_TYPES = ['Stock']
COUNTRIES = ['USA', 'GBR', 'JP', 'DEU', 'FRA']  # ISO 3166-1 alpha-3
CURRENCIES = ['USD', 'CAD', 'EUR', 'GBP', 'JPY']
EXCHANGES = [
    # US
    'NYSE',
    'NASDAQ',
]


def get_securities_eodhd(api_key: str) -> pd.DataFrame:
    df_list = []
    try:
        for exchange in EXCHANGES:
            url = f'https://eodhistoricaldata.com/api/exchange-symbol-list/{exchange}'
            response = requests.get(url, params={'api_token': api_key})
            response.raise_for_status()
            df_list.append(pd.read_csv(StringIO(response.text)))
    except ConnectionError as e:
        logging.error(f"API connection failed: {e}")
        raise
    except HTTPError as e:
        logging.error(f"API request failed: {e}")
        raise
    df = pd.concat(df_list)
    df.columns = df.columns.str.lower()
    df_cleaned = (
        df
        .loc[lambda x: x['code'].notna()]
        .loc[lambda x: x['exchange'].isin(EXCHANGES)]
        .rename(columns={
            'code': 'ticker',
            'type': 'asset_type',
        })
        .replace({'asset_type': {
            'Common Stock': 'Stock',
            'Preferred Stock': 'Stock',
        }}, regex=True)
        .fillna({'isin': ''})
        .loc[lambda x: x['asset_type'] == 'Stock']
        .drop_duplicates(subset=['ticker'])
    )
    return df_cleaned


def get_securities_tiingo(api_key: str) -> pd.DataFrame:
    tiingo_client = TiingoClient({'session': True, 'api_key': api_key})
    df = pd.DataFrame(tiingo_client.list_stock_tickers())
    df_cleaned = (
        df
        .loc[lambda x: x['ticker'].notna()]
        .loc[lambda x: x['startDate'].notna()]
        .loc[lambda x: x['endDate'].notna()]
        .loc[lambda x: x['exchange'].isin(EXCHANGES)]
        .drop(columns=['startDate', 'endDate'], axis=1)
        .rename(columns={
            'assetType': 'asset_type',
            'priceCurrency': 'currency',
        })
        .loc[lambda x: x['asset_type'] == 'Stock']
    )
    return df_cleaned


def get_prices_tiingo(
    api_key: str,
    ticker_ids: Dict[str, int],
    start_date: dt.date,
    end_date: dt.date
) -> pd.DataFrame:
    tiingo_client = TiingoClient({'session': True, 'api_key': api_key})
    tickers = [ticker for ticker, _ in ticker_ids.items()]
    df = tiingo_client.get_dataframe(
        tickers,
        frequency='daily',
        metric_name='close',
        startDate=start_date,
        endDate=end_date,
    )
    df.columns = [
        (col if col == 'index' else ticker_ids[col])
        for col in df.columns
    ]
    df_cleaned = (
        df
        .reset_index()
        .rename(columns={'index': 'date'})
        .melt('date', var_name='ticker_id', value_name='close_price')
        # Just take date part
        .assign(**{'date': lambda x: pd.to_datetime(x['date']).dt.date})
        .astype({'ticker_id': 'int64', 'close_price': 'float64'})
    )
    return df_cleaned


def load_securities_csv() -> None:
    app = current_app._get_current_object()  # type: ignore
    df = pd.read_csv(app.config['ROOT_DIR'] + '/data/securities.csv')
    df.to_sql(
        "securities",
        con=db.engine,
        if_exists="append",
        index=False
    )


def load_securities() -> None:
    app = current_app._get_current_object()  # type: ignore
    API_KEY_TIINGO = app.config['API_KEY_TIINGO']
    API_KEY_EODHD = app.config['API_KEY_EODHD']
    try:
        df_eodhd = get_securities_eodhd(API_KEY_EODHD)
    except:
        return
    df_tiingo = get_securities_tiingo(API_KEY_TIINGO)
    df_cleaned = (
        pd
        .merge(
            df_tiingo,
            df_eodhd,
            on=['ticker', 'exchange', 'asset_type', 'currency'],
            how='inner'
        )
        .drop_duplicates(subset=['ticker', 'exchange', 'asset_type', 'currency'])
        .drop(columns=['asset_type'], axis=1)
    )
    df_cleaned.to_sql(
        "securities",
        con=db.engine,
        if_exists="append",
        index=False
    )
    return


def load_prices(
    tickers: List[str],
    start_date: dt.date,
    end_date: dt.date
) -> None:
    df_tickers = SecurityMgr.get_items(
        filters=[Security.ticker.in_(tickers)],
        entities=[Security.ticker, Security.id],
    )
    if not df_tickers.empty:
        ticker_ids = (
            df_tickers 
            .set_index('ticker')
            .to_dict()
            .get('id', {})
        )
        if ticker_ids:
            app = current_app._get_current_object()  # type: ignore
            API_KEY_TIINGO = app.config['API_KEY_TIINGO']
            df = get_prices_tiingo(
                API_KEY_TIINGO, ticker_ids, start_date, end_date)
            df.to_sql(
                "prices",
                con=db.engine,
                if_exists="append",
                index=False
            )


def load_prices_all_tickers() -> None:
    with scheduler.app.app_context():  # type: ignore
        df_all_tickers = WatchlistItemMgr.get_distinct_items(
            filters=[db.literal(True)],
            distinct_on=[WatchlistItem.ticker],
            entities=[WatchlistItem.ticker],
            orderby=[WatchlistItem.ticker],
        )
        if not df_all_tickers.empty:
            all_tickers = df_all_tickers.loc[:, 'ticker'].to_list()
            if all_tickers:
                end_date = dt.date.today() - dt.timedelta(days=1)
                start_date = end_date
                load_prices(all_tickers, start_date, end_date)


def load_prices_ticker(ticker: str) -> None:
    with scheduler.app.app_context():  # type: ignore
        price_obj = PriceMgr.get_first_item(
            filters=[Security.ticker == ticker],
            orderby = [Price.date.desc()]
        )
        if price_obj is None:
            end_date = dt.date.today() - dt.timedelta(days=1)
            start_date = end_date - dt.timedelta(days=100)
            load_prices([ticker], start_date, end_date)
