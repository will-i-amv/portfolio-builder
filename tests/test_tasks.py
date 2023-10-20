import numpy as np
import pandas as pd
from pandas.api.types import is_object_dtype
import pytest

from portfolio_builder.public.tasks import (
    EXCHANGES, CURRENCIES, COUNTRIES, get_securities_eodhd
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

    # Returns a DataFrame where all country codes are valid
    @pytest.mark.vcr
    def test_returns_dataframe_valid_col_values(self, app):
        # Returns a pandas DataFrame where all the values in the 'country' column 
        # are valid country codes
        with app.app_context():
            api_key = app.config['API_KEY_EODHD']
            df = get_securities_eodhd(api_key)
            assert df['exchange'].isin(EXCHANGES).all()
            assert df['country'].isin(COUNTRIES).all()
            assert df['currency'].isin(CURRENCIES).all()
