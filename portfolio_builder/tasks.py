import random
import time
from io import StringIO

import pandas as pd
import requests
from flask import current_app
from tiingo import TiingoClient

from portfolio_builder import db


EXCHANGES = [
    # US
    'NYSE',
    'NYSE ARCA',
    'NYSE MKT',
    'NASDAQ',
]
app = current_app._get_current_object()
API_KEY_TIINGO = app.config['API_KEY_TIINGO']
API_KEY_EODHD = app.config['API_KEY_EODHD']
tiingo_client = TiingoClient({'session': True, 'api_key': API_KEY_TIINGO})


def get_data_eodhd():
    df_eodhd = pd.DataFrame()
    for exchange in EXCHANGES:
        url = (
            f'https://eodhistoricaldata.com/api/exchange-symbol-list/' +
            f'{exchange}?api_token={API_KEY_EODHD}'
        )
        response = requests.get(url)
        df_eodhd = pd.concat([df_eodhd, pd.read_csv(StringIO(response.text))])
        time.sleep(1.0 + round(random.random(), 2))
    df_eodhd.columns = [col.lower() for col in df_eodhd.columns]
    df_eodhd_cleaned = (
        df_eodhd
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
    return df_eodhd_cleaned


def get_data_tiingo():
    df_tiingo = pd.DataFrame(tiingo_client.list_stock_tickers())
    df_tiingo_cleaned = (
        df_tiingo
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
    return df_tiingo_cleaned


def load_securities():
    if not (API_KEY_TIINGO and API_KEY_EODHD):
        df_cleaned = pd.read_csv(app.config['ROOT_DIR'] + '/data/securities.csv')
    else:
        df_eodhd_cleaned = get_data_eodhd()
        df_tiingo_cleaned = get_data_tiingo()
        df_cleaned = (
            pd
            .merge(
                df_tiingo_cleaned,
                df_eodhd_cleaned,
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
