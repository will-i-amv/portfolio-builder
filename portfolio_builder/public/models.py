import datetime as dt

from portfolio_builder import db


def get_default_date():
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

    def __repr__(self):
        return (
            f"<Security Name: {self.name}, " + 
            f"Ticker Name: {self.ticker}, " + 
            f"Country: {self.country}>"
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

    def __repr__(self):
        return (f"<Watchlist ID: {self.id}, Watchlist Name: {self.name}>")


class WatchlistItem(db.Model):
    __tablename__ = "watchlist_items"
    id = db.Column(db.Integer, primary_key=True, index=True)
    watchlist = db.Column(db.String(25), nullable=False)
    ticker = db.Column(db.String(20), nullable=False)
    quantity = db.Column(db.Integer, nullable=False)
    price = db.Column(db.Float, nullable=False)
    sector = db.Column(db.String(100), nullable=False)
    trade_date = db.Column(db.DateTime, default=get_default_date)
    created_timestamp = db.Column(db.DateTime, default=dt.datetime.utcnow)
    comments = db.Column(db.String(140))
    watchlist_id = db.Column(
        db.Integer,
        db.ForeignKey("watchlists.id", ondelete="CASCADE"),
        nullable=False
    )

    def __repr__(self):
        return (f"<Order ID: {self.id}, Ticker: {self.ticker}>")
