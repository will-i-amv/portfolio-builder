import datetime as dt
import pytest
from sqlalchemy.exc import IntegrityError, DataError

from portfolio_builder.public.models import (
    Security, Price, Watchlist, WatchlistItem, get_watchlists
)


@pytest.fixture(scope='function')
def db_teardown(db):
    yield db
    db.session.query(Price).delete()
    db.session.query(Security).delete()
    db.session.query(Watchlist).delete()
    db.session.query(WatchlistItem).delete()
    db.session.commit()


@pytest.fixture(scope='function')
def with_rollback(db):
    yield
    db.session.rollback()


@pytest.fixture(scope='function')
def security(db):
    security = Security(name="Apple Inc.", ticker="AAPL", exchange="NASDAQ")
    db.session.add(security)
    db.session.commit()
    yield security


class TestSecurity:
    # create a Security instance with valid parameters
    def test_create_valid_instance(self, db, db_teardown):
        security = Security(name="Apple Inc.", ticker="AAPL", exchange="NASDAQ")
        assert security.name == "Apple Inc."
        assert security.ticker == "AAPL"
        assert security.exchange == "NASDAQ"
        price = Price(date=dt.date(2023, 1, 1), close_price=100.0)
        security.prices.append(price)
        assert len(security.prices) == 1 # type: ignore
        assert security.prices[0] == price # type: ignore
        db.session.add(security)
        db.session.commit()
        stored_security = Security.query.get(security.id)
        assert stored_security == security

    def test_create_instance_with_empty_name(self, db, with_rollback):
        with pytest.raises(IntegrityError):
            null_name = Security(name=None, ticker="AAPL", exchange="NASDAQ")
            db.session.add(null_name)
            db.session.commit()

    # create a Security instance with an empty ticker
    def test_create_instance_with_empty_ticker(self, db, with_rollback):
        with pytest.raises(IntegrityError):
            null_ticker = Security(name="Apple Inc.", ticker=None, exchange="NASDAQ")
            db.session.add(null_ticker)
            db.session.commit()

    # create a Security instance with an empty exchange
    def test_create_instance_with_empty_exchange(self, db, with_rollback):
        with pytest.raises(IntegrityError):
            null_exchange = Security(name="Apple Inc.", ticker="AAPL", exchange=None)
            db.session.add(null_exchange)
            db.session.commit()


class TestPrice:

    def test_create_valid_instance(self, db, security):
        price = Price(date=dt.date(2022, 1, 1), close_price=10.0, ticker_id=security.id)
        assert price.date == dt.date(2022, 1, 1)
        assert price.close_price == 10.0
        assert price.ticker_id == 1
        db.session.add(price)
        db.session.commit()
        stored_price = Price.query.get(price.id)
        assert stored_price == price

    def test_create_price_with_null_date(self, db, security, with_rollback):
        with pytest.raises(IntegrityError):
            null_date = Price(date=None, close_price=170.0, ticker_id=security.id)
            db.session.add(null_date)
            db.session.commit()

    def test_create_price_with_null_close_price(self, db, security, with_rollback):
        with pytest.raises(IntegrityError):
            null_price = Price(date=dt.date(2022, 1, 1), close_price=None, ticker_id=security.id)
            db.session.add(null_price)
            db.session.commit()

    def test_create_price_with_null_ticker_id(self, db, with_rollback):
        with pytest.raises(IntegrityError):
            null_ticker_id = Price(date=dt.date(2022, 1, 1), close_price=180.0, ticker_id=None)
            db.session.add(null_ticker_id)
            db.session.commit()


class TestWatchlist:

    def test_add_watchlist_with_items(self, db, db_teardown):
        watchlist = Watchlist(name="My Watchlist", user_id=1)
        assert watchlist.name == "My Watchlist"
        assert watchlist.user_id == 1
        item = WatchlistItem(
            ticker = 'AAPL',
            quantity = 10,
            price = 170.0,
            side = 'buy',
            trade_date = dt.date.today(),
            watchlist_id=watchlist.id,
        )
        watchlist.items.append(item)
        assert len(watchlist.items) == 1 # type: ignore
        assert watchlist.items[0] == item # type: ignore
        db.session.add(watchlist)
        db.session.commit()
        stored_watchlist = Watchlist.query.get(watchlist.id)
        assert stored_watchlist == watchlist

    def test_retrieve_watchlists_for_user(self, db, db_teardown):
        user_id = 1
        watchlist1 = Watchlist(name="Watchlist 1", user_id=user_id)
        watchlist2 = Watchlist(name="Watchlist 2", user_id=user_id)
        db.session.add_all([watchlist1, watchlist2])
        db.session.commit()
        watchlists = get_watchlists([Watchlist.user_id == user_id])
        assert len(watchlists) == 2
        assert watchlists[0] == watchlist1
        assert watchlists[1] == watchlist2

    def test_create_watchlist_with_null_name(self, db, with_rollback):
        with pytest.raises(IntegrityError):
            watchlist = Watchlist(name=None, user_id=1)
            db.session.add(watchlist)
            db.session.commit()

    def test_create_watchlist_with_null_user_id(self, db, with_rollback):
        with pytest.raises(IntegrityError):
            watchlist = Watchlist(name="My Watchlist", user_id=None)
            db.session.add(watchlist)
            db.session.commit()
