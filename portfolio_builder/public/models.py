import datetime as dt
from typing import Any, List, Tuple

from sqlalchemy.sql import expression, func, case
from sqlalchemy.engine.row import Row
from sqlalchemy.sql.elements import BinaryExpression

from portfolio_builder import db


def get_default_date() -> dt.date:
    trade_date = dt.date.today()
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
    side = db.Column(db.String(5), nullable=False)
    trade_date = db.Column(db.Date, nullable=False)
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


def get_securities() -> List[Security]:
    return db.session.query(Security).all()


def get_prices(ticker: str) -> List[Price]:
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


def _filter_watchlist_items(filter):
    query = (
        db
        .session
        .query(WatchlistItem)
        .join(Watchlist, onclause=(WatchlistItem.watchlist_id==Watchlist.id))
        .filter(*filter)
    )
    return query


def get_watch_items(
    filter: List[BinaryExpression],
    select: List[Any] = [WatchlistItem],
    orderby: List[Any] = [WatchlistItem.id]
) -> List[WatchlistItem]:
    query = _filter_watchlist_items(filter)
    items = (
        query
        .with_entities(*select)
        .order_by(*orderby)
        .all()
    )
    return items


def get_grouped_watch_items(filter: List[BinaryExpression]) -> List[Row[Tuple[Any, Any]]]:
    query = _filter_watchlist_items(filter)
    grouped_items = (
        query
        .group_by(func.date(WatchlistItem.trade_date))
        .with_entities(
            func.date(WatchlistItem.trade_date).label('date'),
            func.sum(
                WatchlistItem.quantity * WatchlistItem.price * case(
                    (WatchlistItem.side == 'buy', 1),
                    (WatchlistItem.side == 'sell', (-1)),
                )
            )
            .label('flows')
        )
        .order_by(func.date(WatchlistItem.trade_date))
        .all()
    )
    return grouped_items


def get_watchlists(
    filter: List[BinaryExpression],
    columns: List[Any] = Watchlist
) -> List[Watchlist]:
    watchlists = (
        db
        .session
        .query(Watchlist)
        .filter(*filter)
        .with_entities(*columns)
        .order_by(Watchlist.id)
        .all()
    )    
    return watchlists
