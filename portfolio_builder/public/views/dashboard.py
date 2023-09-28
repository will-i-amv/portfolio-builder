import pandas as pd
from flask import Blueprint, request, render_template
from flask_login import login_required

from portfolio_builder.public.models import (
    Watchlist, WatchlistItem, 
    get_prices, get_watch_tickers, get_watch_trade_history, 
    get_watch_flows, get_all_watch_names
)
from portfolio_builder.public.portfolio import FifoAccounting


bp = Blueprint('dashboard', __name__)


def calc_daily_valuations(ticker, prices, summaries):
    """
    Combines the position breakdown with the daily prices to calculate
    daily market value. The Daily market value is the positions quantity
    multiplied by the market price.
    """
    df_summaries = (
        summaries
        .astype({
            # 'date': 'datetime64[ns]', 
            'date': 'str', 
            'quantity': 'float64', 
            'average_price': 'float64',
        })
    )
    df_prices = (
        pd
        .DataFrame(
            data=prices, 
            columns=["date", "price"]
        )
        .astype({
            # 'date': 'datetime64[ns]', 
            'date': 'str', 
            'price': 'float64'
        })
    )
    df_cleaned = (
        pd
        .merge(df_prices, df_summaries, on=['date'], how='left')
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
    return df_cleaned


def get_position_summary(watchlist_name):
    all_tickers = get_watch_tickers(filter=[
        Watchlist.name == watchlist_name
    ])
    summary_table = {}
    for ticker in all_tickers:
        trade_history = get_watch_trade_history(filter=[
            Watchlist.name == watchlist_name,
            WatchlistItem.ticker == ticker,
        ])
        fifo_accounting = FifoAccounting(trade_history)
        df_summary = (
            pd
            .DataFrame(
                data=fifo_accounting.breakdown, 
                columns=['date', 'quantity', 'average_price']
            )
        )
        summary_table[ticker] = df_summary
    return summary_table


def get_portfolio_summary(all_summaries):
    df = pd.DataFrame()
    for ticker, summaries in all_summaries.items():
        prices = get_prices(ticker)
        pos_valuation = calc_daily_valuations(ticker, prices, summaries)
        if df.empty:
            df = pos_valuation
        else:
            df = df.join(pos_valuation)
        df = df.fillna(method="ffill")
    return df


def convert_flows(flows):
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
        .astype({'date': 'str'})
        .assign(
            cash=lambda x: x.loc[x['flows'] > 0, 'flows'].cumsum(),
            inflows=lambda x: x.loc[x['flows'] <= 0, 'flows'].abs(), # This should also be .cumsum()
        )
        .set_index("date")
        .assign(cash=lambda x: x['cash'].ffill())
        .fillna(0)
        .drop(columns=['flows'])
    )
    return df_flows


def generate_hpr(df_summary, flows):
    """
    Where PortVal = Portfolio Value. The Formula for the Daily
    Holding Period Return (HPR) is calculated as follows:
    (Ending PortVal) / (Previous PortVal After Cash Flow) – 1.

    1. Add the cash from the sale of securities to the portfolio value.
    2. shift the total portfolio value column to allow us to easily
        caclulate the Percentage change before and after each cash flow.
    Returns a named tuple of daily HPR % changes.
    """
    df_flows = convert_flows(flows)
    df_valuation =( 
        df_summary
        .assign(portfolio_val=lambda x: x.sum(axis=1))
        .loc[:, ['portfolio_val']]
    )
    df = (
        pd
        .merge(df_valuation, df_flows, left_index=True, right_index=True, how='left')
        .assign(cash=lambda x: x['cash'].ffill())
        .fillna(0)
        .assign(
            total_portfolio_val=lambda x: x["portfolio_val"] + x["cash"]
        )
        .assign(
            total_portfolio_val_prev=lambda x: x['total_portfolio_val'].shift(1)
        )
        .assign(
            pct_change=lambda x: (
                (
                    x["total_portfolio_val"] / 
                    (x["total_portfolio_val_prev"] + x["inflows"])
                ) - 1
            ) * 100
        )
        .round({'pct_change': 3})
        .fillna({'pct_change': 0.0})
        .loc[:, ['pct_change']]
        .reset_index()
    )
    return list(df.itertuples(index=False))


def get_pie_chart(df_portfolio):
    """
    Returns a named tuple of the largest positions by absolute exposure
    in descending order. For the portfolios that contain more than 6
    positions the next n positons are aggregated to and classified
    as 'Other'
    """
    if df_portfolio.empty:
        return list(df_portfolio.itertuples(index=False))
    df_initial = (
        df_portfolio
        .tail(1)
        .T
        .reset_index()
    )
    df_initial.columns = ['ticker', 'Market_val']
    df_temp = (
        df_initial
        .assign(Market_val=lambda x: x["Market_val"].abs())
        .replace({'ticker': {'market_val_': ''}}, regex=True)
        .assign(
            market_val_perc=lambda x: 
                (x["Market_val"]  / x["Market_val"].sum()) * 100
        )
        .round({'market_val_perc': 2})
        .loc[lambda x: x["Market_val"] != 0.0]
        .sort_values(by=['market_val_perc'], ascending=False)
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


def get_bar_chart(df_portfolio):
    """
    Returns a named tuple of the 5 largest positions by absolute exposure
    in descending order
    """
    if df_portfolio.empty:
        return list(df_portfolio.itertuples(index=False))
    df_initial = (
        df_portfolio
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


@bp.route('/', methods=['GET', 'POST'])
@login_required
def index():
    watch_names = get_all_watch_names()
    if request.method == 'POST':
        curr_watch_name = request.form.get('watchlist_group_selection')
    else:
        curr_watch_name = next(iter(watch_names), '')
    summaries = get_position_summary(curr_watch_name)
    summary = [
        (
            df
            .assign(ticker=ticker)
            .sort_values(by=['date'])
            .drop(['date'], axis=1)
            .tail(1)
            .to_dict('records')[0]
        ) 
        for ticker, df in summaries.items()
    ]
    if len(summary) > 7:
        summary = summary[0:7]
    flows = get_watch_flows(filter=[Watchlist.name == curr_watch_name])
    portfolio = get_portfolio_summary(summaries)
    portfolio_hpr = generate_hpr(portfolio, flows)
    content = {
        'summary': summary, 
        'line_chart': portfolio_hpr,
        'pie_chart': get_pie_chart(portfolio), 
        'bar_chart': get_bar_chart(portfolio),
        'watch_names': watch_names, 
        'curr_watch_name': curr_watch_name
    }
    return render_template('public/dashboard.html', **content)
