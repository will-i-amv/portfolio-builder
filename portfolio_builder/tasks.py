import random
import time
from io import StringIO

import pandas as pd
import requests
from flask import current_app
from tiingo import TiingoClient

from portfolio_builder import db
from portfolio_builder.public.models import Security


EXCHANGES = [
    # US
    'NYSE',
    'NYSE ARCA',
    'NYSE MKT',
    'NASDAQ',
]


def get_securities_eodhd(api_key):
    df = pd.DataFrame()
    for exchange in EXCHANGES:
        url = (
            f'https://eodhistoricaldata.com/api/exchange-symbol-list/' +
            f'{exchange}?api_token={api_key}'
        )
        response = requests.get(url)
        df = pd.concat([df, pd.read_csv(StringIO(response.text))])
        time.sleep(1.0 + round(random.random(), 2))
    df.columns = [col.lower() for col in df.columns]
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
    )
    return df_cleaned


def get_securities_tiingo(api_key):
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


def get_prices_tiingo(api_key, ticker_ids, start_date, end_date):
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
        .assign(**{'date': lambda x: pd.to_datetime(x['date'])})
        .astype({'ticker_id': 'int64', 'close_price': 'float64'})
    )
    return df_cleaned


def load_securities():
    app = current_app._get_current_object()
    API_KEY_TIINGO = app.config['API_KEY_TIINGO']
    API_KEY_EODHD = app.config['API_KEY_EODHD']
    if not (API_KEY_TIINGO and API_KEY_EODHD):
        df_cleaned = pd.read_csv(app.config['ROOT_DIR'] + '/data/securities.csv')
    else:
        df_eodhd = get_securities_eodhd(API_KEY_EODHD)
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


def load_prices(tickers, start_date, end_date):
    app = current_app._get_current_object()
    API_KEY_TIINGO = app.config['API_KEY_TIINGO']
    ticker_ids = dict(
        db
        .session
        .query(Security)
        .filter(Security.ticker.in_(tickers))
        .with_entities(Security.ticker, Security.id)
        .all()
    )
    df = get_prices_tiingo(API_KEY_TIINGO, ticker_ids, start_date, end_date)
    df.to_sql(
        "prices",
        con=db.engine,
        if_exists="append",
        index=False
    )
