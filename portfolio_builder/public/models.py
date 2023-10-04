import datetime as dt
from typing import List

from flask_login import current_user
from sqlalchemy.sql import expression, func
from sqlalchemy.engine.row import Row
from sqlalchemy.sql.elements import BinaryExpression

from portfolio_builder import db


def get_default_date() -> dt.datetime:
    trade_date = dt.datetime.utcnow()
    weekday = dt.date.isoweekday(trade_date)
    if weekday == 6: # Saturday
        trade_date = trade_date - dt.timedelta(days=1)
    elif weekday == 7: # Sunday
        trade_date = trade_date - dt.timedelta(days=2)
    return trade_date


class Security(db.Model):
    __tablename__ = "securities"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    name = db.Column(db.String(200), nullable=False)
    ticker = db.Column(db.String(10), nullable=False)
    exchange = db.Column(db.String(10), nullable=False)
    currency = db.Column(db.String(3))
    country = db.Column(db.String(40))
    isin = db.Column(db.String(20))
    prices = db.relationship(
        "Price",
        backref="securities", 
        passive_deletes=True
    )

    def __repr__(self) -> str:
        return (
            f"<Security Name: {self.name}, " + 
            f"Ticker Name: {self.ticker}, " + 
            f"Country: {self.country}>"
        )


class Price(db.Model):
    __tablename__ = "prices"
    __table_args__ = (
        db.Index("idx_date_tickerid", 'date', 'ticker_id'),
        db.Index("idx_tickerid_date", 'ticker_id', 'date'),
    )
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    date = db.Column(db.Date, nullable=False)
    close_price = db.Column(db.Numeric(11, 6), nullable=False)
    ticker_id = db.Column(
        db.Integer,
        db.ForeignKey("securities.id"), 
        nullable=False
    )

    def __repr__(self) -> str:
        return (
            f"<Date: {self.date}, " + 
            f"Ticker ID: {self.ticker_id}, " + 
            f"Close Price: {self.close_price}>"
        )


class Watchlist(db.Model):
    __tablename__ = "watchlists"
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(25), nullable=False)
    user_id = db.Column(
        db.Integer,
        db.ForeignKey("users.id", ondelete="CASCADE"), 
        nullable=False
    )
    items = db.relationship(
        "WatchlistItem",
        backref="watchlists", 
        passive_deletes=True
    )

    def __repr__(self) -> str:
        return (f"<Watchlist ID: {self.id}, Watchlist Name: {self.name}>")


class WatchlistItem(db.Model):
    __tablename__ = "watchlist_items"
    id = db.Column(db.Integer, primary_key=True, index=True)
    ticker = db.Column(db.String(20), nullable=False)
    quantity = db.Column(db.Integer, nullable=False)
    price = db.Column(db.Float, nullable=False)
    trade_date = db.Column(db.DateTime, default=get_default_date)
    is_last_trade = db.Column(db.Boolean, server_default=expression.true(), nullable=False)
    created_timestamp = db.Column(db.DateTime, default=dt.datetime.utcnow)
    comments = db.Column(db.String(140))
    watchlist_id = db.Column(
        db.Integer,
        db.ForeignKey("watchlists.id", ondelete="CASCADE"),
        nullable=False
    )

    def __repr__(self) -> str:
        return (f"<Order ID: {self.id}, Ticker: {self.ticker}>")


def get_prices(ticker: str) -> List[Row]:
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


def _watchlist_items_query(filter):
    query = (
        db
        .session
        .query(WatchlistItem)
        .join(Watchlist, onclause=(WatchlistItem.watchlist_id==Watchlist.id))
        .filter(
            Watchlist.user_id == current_user.id,
            *filter
        )
    )
    return query


def get_watch_items(filter: List[BinaryExpression]) -> List[Row]:
    query = _watchlist_items_query(filter)
    items = query.all()
    return items


def get_watch_tickers(filter: List[BinaryExpression]) -> List[str]:
    query = _watchlist_items_query(filter)
    tickers = (
        query
        .with_entities(WatchlistItem.ticker)
        .distinct(WatchlistItem.ticker)
        .order_by(WatchlistItem.ticker)
        .all()
    )
    return [item.ticker for item in tickers]


def get_watch_trade_history(filter: List[BinaryExpression]) -> List[Row]:
    query = _watchlist_items_query(filter)
    history = (
        query
        .with_entities(
            WatchlistItem.ticker,
            WatchlistItem.quantity,
            WatchlistItem.price,
            func.date(WatchlistItem.trade_date).label("date")
        )
        .order_by(WatchlistItem.trade_date)
        .all()
    )
    return history


def get_watch_flows(filter: List[BinaryExpression]) -> List[Row]:
    query = _watchlist_items_query(filter)
    flows = (
        query
        .group_by(func.date(WatchlistItem.trade_date))
        .with_entities(
            func.date(WatchlistItem.trade_date).label('index'),
            func.sum(WatchlistItem.quantity * WatchlistItem.price * (-1)).label('flows')
        )
        .order_by(func.date(WatchlistItem.trade_date))
        .all()
    )
    return flows


def get_all_watch_names() -> List[str]:
    watchlists = (
        db
        .session
        .query(Watchlist)
        .with_entities(Watchlist.name)
        .filter_by(user_id=current_user.id)
        .order_by(Watchlist.id)
        .all()
    )
    return [item[0] for item in watchlists]
