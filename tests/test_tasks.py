import numpy as np
import pandas as pd
import pytest

from portfolio_builder.public.tasks import (
    EXCHANGES, CURRENCIES, COUNTRIES, ASSET_TYPES, 
    get_securities_eodhd
)


class TestGetSecuritiesEodhd:

    @pytest.mark.vcr
    def test_returns_valid_dataframe(self, app):
        # Returns a pandas DataFrame with the expected columns and data types
        with app.app_context():
            api_key = app.config['API_KEY_EODHD']
            df = get_securities_eodhd(api_key)
            expected_columns = [
                'ticker', 
                'name', 
                'country', 
                'exchange',
                'currency',
                'asset_type',
                'isin',
            ]
            assert isinstance(df, pd.DataFrame)
            assert not df.empty
            assert df.columns.tolist() == expected_columns
            assert [
                isinstance(col, type(np.dtype('object'))) 
                for col in df.dtypes
            ]

    @pytest.mark.vcr
    def test_returns_dataframe_nonempty_values(self, app):
        # Returns a pandas DataFrame where none of the values in 
        # any of the columns are empty or null
        with app.app_context():
            api_key = app.config['API_KEY_EODHD']
            df = get_securities_eodhd(api_key)
            assert df.isnull().sum().sum() == 0

    @pytest.mark.vcr
    def test_returns_dataframe_valid_col_values(self, app):
        # Returns a pandas DataFrame where all the column values 
        # are valid ones
        with app.app_context():
            api_key = app.config['API_KEY_EODHD']
            df = get_securities_eodhd(api_key)
            # The dataframe has unique tickers
            assert df['name'].apply(lambda x: isinstance(x, str) and x != '').all()
            assert df['ticker'].nunique() == len(df) 
            assert df['exchange'].isin(EXCHANGES).all()
            assert df['country'].isin(COUNTRIES).all()
            assert df['currency'].isin(CURRENCIES).all()
            assert df['asset_type'].isin(ASSET_TYPES).all()
            # The 'isin' column is a string of length 12 
            # if the ticker has an ISIN, or 0 if it doesn't
            assert df['isin'].str.len().isin([12, 0]).all()
