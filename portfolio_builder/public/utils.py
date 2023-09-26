import pandas as pd
from flask_login import current_user
from sqlalchemy.sql import func

from portfolio_builder import db
from portfolio_builder.public.models import (
    Watchlist, WatchlistItem, Price, Security
)
from portfolio_builder.public.portfolio import (
    PositionSummary, PortfolioSummary, calc_daily_valuations
)


def get_prices(ticker):
    prices = (
        db
        .session
        .query(Price)
        .join(Security, onclause=(Price.ticker_id==Security.id))
        .filter(Security.ticker == ticker)
        .with_entities(Price.date, Price.close_price)
        .all()
    )
    return prices


def get_watchlist_tickers(watchlist_name):
    tickers = (
        db
        .session
        .query(WatchlistItem)
        .join(Watchlist, onclause=(WatchlistItem.watchlist_id==Watchlist.id))
        .filter(
            Watchlist.user_id == current_user.id,
            Watchlist.name == watchlist_name
        )
        .with_entities(WatchlistItem.ticker)
        .distinct(WatchlistItem.ticker)
        .order_by(WatchlistItem.ticker)
        .all()
    )
    return [item.ticker for item in tickers]


def get_trade_history(watchlist_name, ticker):
    trade_history = (
        db
        .session
        .query(WatchlistItem)
        .join(Watchlist, onclause=(WatchlistItem.watchlist_id==Watchlist.id))
        .filter(
            Watchlist.user_id == current_user.id,
            Watchlist.name == watchlist_name,
            WatchlistItem.ticker == ticker,
        )
        .with_entities(
            WatchlistItem.ticker,
            WatchlistItem.quantity,
            WatchlistItem.price,
            func.date(WatchlistItem.trade_date).label("date")
        )
        .order_by(WatchlistItem.trade_date)
        .all()
    )
    return trade_history


def get_portfolio_flows(watchlist_name):
    flows = (
        db
        .session
        .query(WatchlistItem)
        .join(Watchlist, onclause=(WatchlistItem.watchlist_id==Watchlist.id))
        .with_entities(
            func.date(WatchlistItem.trade_date).label('index'),
            func.sum(WatchlistItem.quantity * WatchlistItem.price * (-1)).label('flows')
        )
        .filter(
            Watchlist.user_id == current_user.id,
            Watchlist.name == watchlist_name
        )
        .group_by(func.date(WatchlistItem.trade_date))
        .order_by(func.date(WatchlistItem.trade_date))
        .all()
    )
    return flows


def get_position_summary(watchlist_name):
    all_tickers = get_watchlist_tickers(watchlist_name)
    summary_table = {}
    for ticker in all_tickers:
        trade_history = get_trade_history(watchlist_name, ticker)
        summary = PositionSummary(trade_history).get_summary()
        summary_table[ticker] = summary
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
    Portfolio = PortfolioSummary(df)
    return Portfolio
