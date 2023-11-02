from typing import Any, Dict, List

import pandas as pd
from flask import Blueprint, request, render_template
from flask_login import login_required, current_user
from sqlalchemy.engine.row import Row

from portfolio_builder.public.models import (
    Watchlist, WatchlistItem, Security,
    get_prices, get_watchlists, get_watch_items, get_grouped_watch_items
)
from portfolio_builder.public.portfolio import FifoAccounting


bp = Blueprint('dashboard', __name__)


def calc_portf_val_daily(
    ticker: str, 
    df_prices: pd.DataFrame, 
    df_positions: pd.DataFrame
) ->  pd.DataFrame:
    """
    Combines the position breakdown with the daily prices to calculate
    daily market value. The Daily market value is the positions quantity
    multiplied by the market price.
    """
    df_portf_val = (
        pd
        .merge(df_prices, df_positions, on=['date'], how='left')
        .astype({
            'quantity': 'float64', 
            'price': 'float64',
        })
        .fillna(method='ffill')
        .assign(market_val=lambda x: x['quantity'] * x['price'])
        .round({'market_val': 3})
        .loc[:, ['date', 'market_val']]
        .rename(columns={'market_val': f'market_val_{ticker}'})
        .set_index('date')
        .dropna()
    )
    return df_portf_val


def get_portf_positions(watchlist_name: str) -> Dict[str, pd.DataFrame]:
    tickers = set([
        item.ticker 
        for item in get_watch_items(
            filters=[
                Watchlist.user_id==1, # type: ignore
                Watchlist.name == 'Technology'
            ],
            entities=[WatchlistItem.ticker],
            orderby=[WatchlistItem.ticker]
        )
    ])
    portf_pos = {}
    for ticker in tickers:
        trade_history = get_watch_items(
            filters=[
                Watchlist.user_id==current_user.id, # type: ignore
                Watchlist.name == watchlist_name,
                WatchlistItem.ticker == ticker,
            ],
            entities=[
                WatchlistItem.ticker,
                WatchlistItem.quantity,
                WatchlistItem.price,
                WatchlistItem.trade_date.label("date")
            ],
            orderby=[WatchlistItem.trade_date]
        )
        fifo_accounting = FifoAccounting(trade_history)
        fifo_accounting.calc_fifo()
        df_positions = (
            pd
            .DataFrame(
                data=fifo_accounting.breakdown, 
                columns=['date', 'quantity', 'average_cost']
            )
            .astype({
                'date': 'datetime64[ns]', 
                'quantity': 'float64', 
                'average_cost': 'float64',
            })
        )
        portf_pos[ticker] = df_positions
    return portf_pos


def get_portf_valuations(portf_pos: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    df_portf_val = pd.DataFrame()
    for ticker, df_positions in portf_pos.items():
        prices = get_prices(filters=[Security.ticker == ticker])
        df_prices = (
            pd
            .DataFrame(
                data=prices, 
                columns=["date", "price"]
            )
            .astype({
                'date': 'datetime64[ns]', 
                'price': 'float64'
            })
        )
        df = calc_portf_val_daily(ticker, df_prices, df_positions)
        if df_portf_val.empty:
            df_portf_val = df
        else:
            df_portf_val = df_portf_val.join(df)
        df_portf_val = df_portf_val.fillna(method="ffill")
    return df_portf_val


def calc_portf_flows_adjusted(flows: List[Row]) -> pd.DataFrame:
    """
    Using the Holding Period Return (HPR) methodology. Purchases of
    securities are accounted as fund inflows and the sale of securities are
    accounted as increases in cash.

    By creating the cumulative sum of these values we can maintain an
    accurate calculation of the HPR which can be distorted as purchases and
    sells are added to the trades.
    """
    df_flows = (
        pd
        .DataFrame(flows, columns=["date", "flows"])
        .astype({'date': 'datetime64[ns]'})
        .assign(
            inflows=lambda x: x.loc[x['flows'] > 0, 'flows'].cumsum(),
            cash=lambda x: x.loc[x['flows'] <= 0, 'flows'].abs().cumsum(),
        )
        .set_index("date")
        .assign(cash=lambda x: x['cash'].ffill())
        .fillna(0)
        .drop(columns=['flows'])
    )
    return df_flows


def calc_portf_hpr(
    df_portf_val: pd.DataFrame, 
    df_portf_flows: pd.DataFrame
) -> List[tuple[Any, ...]]:
    """
    Where PortVal = Portfolio Value. The Formula for the Daily
    Holding Period Return (HPR) is calculated as follows:
    (Ending PortVal) / (Previous PortVal After Cash Flow) – 1.

    1. Add the cash from the sale of securities to the portfolio value.
    2. shift the total portfolio value column to allow us to easily
        caclulate the Percentage change before and after each cash flow.
    Returns a named tuple of daily HPR % changes.
    """
    df_portf_val_filtered =( 
        df_portf_val
        .assign(portf_val=lambda x: x.sum(axis=1))
        .loc[:, ['portf_val']]
    )
    df_portf_hpr = (
        pd
        .merge(
            df_portf_val_filtered, 
            df_portf_flows, 
            left_index=True, 
            right_index=True, 
            how='left'
        )
        .assign(cash=lambda x: x['cash'].ffill())
        .fillna(0)
        .assign(
            total_portf_val=lambda x: x["portf_val"] + x["cash"]
        )
        .assign(
            total_portf_val_prev=lambda x: x['total_portf_val'].shift(1)
        )
        .assign(
            pct_change=lambda x: (
                (
                    x["total_portf_val"] / 
                    (x["total_portf_val_prev"] + x["inflows"])
                ) - 1
            ) * 100
        )
        .round({'pct_change': 3})
        .fillna({'pct_change': 0.0})
        .loc[:, ['pct_change']]
        .reset_index()
    )
    return list(df_portf_hpr.itertuples(index=False))


def get_pie_chart(df_portf_val: pd.DataFrame) -> List[tuple[Any, ...]]:
    """
    Returns a named tuple of the largest positions by absolute exposure
    in descending order. For the portfolios that contain more than 6
    positions the next n positons are aggregated to and classified
    as 'Other'
    """
    if df_portf_val.empty:
        return list(df_portf_val.itertuples(index=False))
    df_initial = (
        df_portf_val
        .tail(1)
        .T
        .reset_index()
    )
    df_initial.columns = ['ticker', 'market_val']
    df_temp = (
        df_initial
        .assign(market_val=lambda x: x["market_val"].abs())
        .replace({'ticker': {'market_val_': ''}}, regex=True)
        .assign(
            market_val_pct=lambda x: 
                (x["market_val"]  / x["market_val"].sum()) * 100
        )
        .round({'market_val_pct': 2})
        .loc[lambda x: x["market_val"] != 0.0]
        .sort_values(by=['market_val_pct'], ascending=False)
    )
    max_len = 6
    df_len = df_temp.shape[0] 
    if df_len < max_len:
        return list(df_temp.itertuples(index=False))
    else:
        df_top = df_temp.head(max_len)
        df_bottom = (
            df_temp
            .tail(df_len - max_len)
            .pivot_table(
                index='ticker',
                margins=True,
                margins_name='Other', # defaults to 'All'
                aggfunc='sum'
            )
            .reset_index()
            .tail(1)
        )
        df_final = pd.concat([df_top, df_bottom])
        return list(df_final.itertuples(index=False))


def get_bar_chart(df_portf_val: pd.DataFrame) -> List[tuple[Any, ...]]:
    """
    Returns a named tuple of the 5 largest positions by absolute exposure
    in descending order
    """
    if df_portf_val.empty:
        return list(df_portf_val.itertuples(index=False))
    df_initial = (
        df_portf_val
        .tail(1)
        .T
        .reset_index()
    )
    df_initial.columns = ['ticker', 'market_val']
    df_final = (
        df_initial
        .replace({'ticker': {'market_val_': ''}}, regex=True)
        .sort_values(by=['market_val'], ascending=False)
        .loc[lambda x: x["market_val"] != 0.0]
        .tail(5)
    )
    return list(df_final.itertuples(index=False))


def get_last_portf_position(portf_pos: Dict[str, pd.DataFrame]) -> List[tuple[Any, ...]]:
    last_portf_pos = []
    for ticker, df_positions in portf_pos.items():
        last_pos = list(
            df_positions
            .assign(ticker=ticker)
            .sort_values(by=['date'])
            .drop(['date'], axis=1)
            .tail(1)
            .itertuples(index=False)
        )
        last_portf_pos.append(*last_pos)
    if len(last_portf_pos) > 7:
        return last_portf_pos[0:7]
    return last_portf_pos


@bp.route('/', methods=['GET', 'POST'])
@login_required
def index() -> str:
    watch_names = [
        item.name
        for item in get_watchlists(
            filters=[Watchlist.user_id==current_user.id], # type: ignore 
        )
    ]
    if request.method == 'POST':
        curr_watch_name = request.form.get('watchlist_group_selection', '')
    else:
        curr_watch_name = next(iter(watch_names), '')
    portf_pos = get_portf_positions(curr_watch_name)
    df_portf_val = get_portf_valuations(portf_pos)
    portf_flows = get_grouped_watch_items(filters=[Watchlist.name == curr_watch_name])
    df_portf_flows = calc_portf_flows_adjusted(portf_flows)
    return render_template(
        'public/dashboard.html',
        summary=get_last_portf_position(portf_pos), 
        line_chart=calc_portf_hpr(df_portf_val, df_portf_flows),
        pie_chart=get_pie_chart(df_portf_val), 
        bar_chart=get_bar_chart(df_portf_val),
        watch_names=watch_names, 
        curr_watch_name=curr_watch_name,
    )
