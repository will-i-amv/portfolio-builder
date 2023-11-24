from collections import deque
from typing import Any, List

import pandas as pd
from flask import Blueprint, request, render_template
from flask_login import login_required, current_user

from portfolio_builder.public.models import (
    Watchlist, WatchlistItem, Security, Price,
    PriceMgr, WatchlistMgr, WatchlistItemMgr
)


bp = Blueprint('dashboard', __name__)


def calc_fifo(df: pd.DataFrame) -> pd.DataFrame:
    df2 = (
        df
        .loc[:, ['ticker', 'date']]
        .assign(net_quantity=0, realized_pnl=0.0)
    )
    net_quantity = 0
    realized_pnl = 0
    inventory = deque()  # Queue to track the inventory of stocks
    for idx, row in df.iterrows():
        if row['side'] == 'buy':
            inventory.append({
                'quantity': row['quantity'],
                'price': row['price']
            })
            net_quantity += row['quantity']
        else:
            last_sold_qty = row['quantity']
            last_sold_price = row['price']
            remaining_qty = last_sold_qty
            while remaining_qty > 0 and inventory:
                first_item = next(iter(inventory))
                first_purchase_qty = first_item['quantity']
                first_purchase_price = first_item['price']
                if first_purchase_qty >= remaining_qty:
                    # The selling quantity is entirely covered by the earliest buying transaction
                    realized_pnl += remaining_qty * (
                        last_sold_price - first_purchase_price
                    )
                    first_item['quantity'] = first_purchase_qty - remaining_qty
                    net_quantity -= remaining_qty
                    remaining_qty = 0
                else:
                    # The selling quantity exceeds the earliest buying transaction
                    realized_pnl += first_purchase_qty * (
                        last_sold_price - first_purchase_price
                    )
                    remaining_qty -= first_purchase_qty
                    net_quantity -= first_purchase_qty
                    inventory.popleft()
        df2.at[idx, 'net_quantity'] = net_quantity
        df2.at[idx, 'realized_pnl'] = realized_pnl
    return df2


def calc_portf_positions(df: pd.DataFrame) -> pd.DataFrame:
    dfs_by_ticker = []
    for ticker in df['ticker'].unique():
        df_temp = calc_fifo(df[lambda x: x['ticker'] == ticker])
        dfs_by_ticker.append(df_temp)
    df_positions = pd.concat(dfs_by_ticker)
    return df_positions


def calc_portf_valuations(
    df_portf_pos: pd.DataFrame,
    df_prices: pd.DataFrame
) -> pd.DataFrame:
    """
    Combines the position breakdown with the daily prices to calculate
    daily market value. The Daily market value is the positions quantity
    multiplied by the market price.
    """
    df_portf_val = (
        pd
        .merge(df_portf_pos, df_prices, on=['ticker', 'date'], how='outer')
        .astype({
            'net_quantity': 'float64',
            'price': 'float64',
        })
        .sort_values(by=['ticker', 'date'])
        .fillna(method='ffill')
        .assign(market_val=lambda x: x['net_quantity'] * x['price'])
        .round({'market_val': 3})
        .loc[:, ['date', 'ticker', 'market_val']]
        .pivot_table(
            index='date',
            columns='ticker',
            values='market_val',
        )
    )
    return df_portf_val


def calc_portf_flows_adjusted(df_flows: pd.DataFrame) -> pd.DataFrame:
    """
    Using the Holding Period Return (HPR) methodology. Purchases of
    securities are accounted as fund inflows and the sale of securities are
    accounted as increases in cash.

    By creating the cumulative sum of these values we can maintain an
    accurate calculation of the HPR which can be distorted as purchases and
    sells are added to the trades.
    """
    df_flows_adj = (
        df_flows
        .assign(
            inflows=lambda x: x.loc[x['flows'] > 0, 'flows'].cumsum(),
            cash=lambda x: x.loc[x['flows'] <= 0, 'flows'].abs().cumsum(),
        )
        .set_index("date")
        .assign(cash=lambda x: x['cash'].ffill())
        .fillna(0)
        .drop(columns=['flows'])
    )
    return df_flows_adj


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
    df_portf_val_filtered = (
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


def calc_last_portf_val(
    df_portf_val: pd.DataFrame,
    no_assets: int = 10
) -> List[tuple[Any, ...]]:
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
        .sort_index()
        .tail(1)
        .T
        .set_axis(['market_val'], axis=1)
        .reset_index()
        .assign(
            market_val_pct=lambda x:
                (x["market_val"] / x["market_val"].sum()) * 100
        )
        .round({'market_val_pct': 2})
        .sort_values(by=['market_val_pct'], ascending=False)
    )
    max_no_assets = df_initial.shape[0]
    if no_assets > max_no_assets:
        return list(df_initial.itertuples(index=False))
    else:
        df_top = df_initial.head(no_assets)
        df_bottom = (
            df_initial
            .tail(max_no_assets - no_assets)
            .pivot_table(
                index='ticker',
                margins=True,
                margins_name='Other',  # defaults to 'All'
                aggfunc='sum'
            )
            .reset_index()
            .tail(1)
        )
        df_final = pd.concat([df_top, df_bottom])
        return list(df_final.itertuples(index=False))


def calc_last_portf_position(
    df_portf_pos: pd.DataFrame,
    no_assets: int = 10
) -> List[tuple[Any, ...]]:
    if df_portf_pos.empty:
        return list(df_portf_pos.itertuples(index=False))
    last_portf_pos = list(
        df_portf_pos
        .loc[lambda x: x.groupby('ticker')['date'].idxmax()]
        .drop(['date'], axis=1)
        .itertuples(index=False)
    )
    max_no_assets = len(last_portf_pos)
    if max_no_assets > no_assets:
        return last_portf_pos[:no_assets]
    return last_portf_pos


@bp.route('/', methods=['GET', 'POST'])
@login_required
def index() -> str:
    df_watch_names = WatchlistMgr.get_items(filters=[
        Watchlist.user_id == current_user.id  # type: ignore
    ])
    watch_names = df_watch_names.loc[:, 'name'].to_list()
    if request.method == 'POST':
        curr_watch_name = request.form.get('watchlist_group_selection', '')
    else:
        curr_watch_name = next(iter(watch_names), '')
    df_trade_history = (
        WatchlistItemMgr
        .get_items(
            filters=[
                Watchlist.user_id == current_user.id,  # type: ignore
                Watchlist.name == curr_watch_name,
            ],
            entities=[
                WatchlistItem.ticker,
                WatchlistItem.quantity,
                WatchlistItem.price,
                WatchlistItem.side,
                WatchlistItem.trade_date.label("date")
            ],
            orderby=[WatchlistItem.ticker, WatchlistItem.trade_date]
        )
        .astype({'date': 'datetime64[ns]'})
     )
    tickers = df_trade_history['ticker'].unique()
    min_date = df_trade_history['date'].dt.date.min()
    max_date = df_trade_history['date'].dt.date.max()
    df_prices = (
        PriceMgr
        .get_items(
            filters=[
                Security.ticker.in_(tickers),
                Price.date.between(min_date, max_date)
            ],
            entities=[
                Security.ticker, 
                Price.date, 
                Price.close_price.label('price'),
            ],
            orderby=[Security.ticker, Price.date]
        )
        .astype({'date': 'datetime64[ns]'})
    )
    df_portf_pos = calc_portf_positions(df_trade_history)
    df_portf_val = calc_portf_valuations(df_portf_pos, df_prices)
    df_portf_flows = (
        WatchlistItemMgr
        .get_grouped_items(filters=[
            Watchlist.user_id == current_user.id,  # type: ignore
            Watchlist.name == curr_watch_name
        ])
        .astype({'date': 'datetime64[ns]'})
    )
    df_portf_flows_adj = calc_portf_flows_adjusted(df_portf_flows)
    df_portf_hpr = calc_portf_hpr(df_portf_val, df_portf_flows_adj)
    df_portf_pos_summary = calc_last_portf_position(df_portf_pos)
    df_portf_val_summary = calc_last_portf_val(df_portf_val)
    return render_template(
        'public/dashboard.html',
        summary=df_portf_pos_summary,
        line_chart=df_portf_hpr,
        pie_chart=df_portf_val_summary,
        bar_chart=df_portf_val_summary,
        watch_names=watch_names,
        curr_watch_name=curr_watch_name,
    )
