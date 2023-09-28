import pandas as pd

from portfolio_builder.public.models import (
    Watchlist, WatchlistItem, 
    get_watch_tickers, get_watch_trade_history, get_prices
)
from portfolio_builder.public.portfolio import (
    calc_daily_valuations, FifoAccounting
)


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
