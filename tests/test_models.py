import datetime as dt
import random

import pytest
from sqlalchemy.exc import IntegrityError

from portfolio_builder.public.models import (
    Security, Price, Watchlist, WatchlistItem, 
    get_securities, get_watchlists
)


@pytest.fixture(scope='function')
def securities(db):
    securities = [
        Security(name="Apple Inc.", ticker="AAPL", exchange="NASDAQ"),
        Security(name="Amazon.com Inc.", ticker="AMZN", exchange="NASDAQ"),
        Security(name="Microsoft Corporation", ticker="MSFT", exchange="NASDAQ"),
    ]
    db.session.add_all(securities)
    db.session.commit()
    yield securities


@pytest.fixture(scope='function')
def prices(db, securities):
    aapl_sec = securities[0]
    amzn_sec = securities[1]
    msft_sec = securities[2]
    prices = [
        Price(date=dt.date(2023, 10, 10), close_price=175.0, ticker_id=aapl_sec.id),
        Price(date=dt.date(2023, 10, 11), close_price=180.0, ticker_id=aapl_sec.id),
        Price(date=dt.date(2023, 110, 12), close_price=170.0, ticker_id=aapl_sec.id),
        Price(date=dt.date(2023, 10, 10), close_price=131.0, ticker_id=amzn_sec.id),
        Price(date=dt.date(2023, 10, 11), close_price=126.0, ticker_id=amzn_sec.id),
        Price(date=dt.date(2023, 110, 12), close_price=132.0, ticker_id=amzn_sec.id),
        Price(date=dt.date(2023, 10, 10), close_price=331.0, ticker_id=msft_sec.id),
        Price(date=dt.date(2023, 10, 11), close_price=334.0, ticker_id=msft_sec.id),
        Price(date=dt.date(2023, 110, 12), close_price=330.0, ticker_id=msft_sec.id),
    ]
    db.session.add_all(prices)
    db.session.commit()
    yield prices


@pytest.fixture(scope='function')
def db_rollback(db):
    yield
    db.session.rollback()


@pytest.fixture(scope='function')
def db_teardown(db):
    yield db
    db.session.query(Price).delete()
    db.session.query(Security).delete()
    db.session.query(Watchlist).delete()
    db.session.query(WatchlistItem).delete()
    db.session.commit()


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

    def test_create_instances_with_null_values(self, db, db_teardown):
        for attr in ['name', 'ticker', 'exchange']:
            with pytest.raises(IntegrityError):
                invalid_sec = Security(name="Apple Inc.", ticker="AAPL", exchange="NASDAQ")
                setattr(invalid_sec, attr, None)
                db.session.add(invalid_sec)
                db.session.commit()
            db.session.rollback()


class TestPrice:

    def test_create_valid_instance(self, db, securities):
        sec = random.choice(securities)
        price = Price(date=dt.date(2022, 1, 1), close_price=10.0, ticker_id=sec.id)
        assert price.date == dt.date(2022, 1, 1)
        assert price.close_price == 10.0
        assert price.ticker_id == 1
        db.session.add(price)
        db.session.commit()
        stored_price = Price.query.get(price.id)
        assert stored_price == price

    def test_create_instances_with_null_values(self, db, securities, db_teardown):
        sec = random.choice(securities)
        for attr in ['date', 'close_price', 'ticker_id']:
            with pytest.raises(IntegrityError):
                invalid_price = Price(
                    date=dt.date(2023, 10, 12), 
                    close_price=200.0, 
                    ticker_id=sec.id
                )
                setattr(invalid_price, attr, None)
                db.session.add(invalid_price)
                db.session.commit()
            db.session.rollback()


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
        watchlists = [watchlist1, watchlist2]
        db.session.add_all(watchlists)
        db.session.commit()
        stored_watchlists = get_watchlists(
            filter=[Watchlist.user_id == user_id],
            select=[Watchlist.name, Watchlist.user_id]
        )
        assert len(watchlists) == 2
        for stored_watch, watch in zip(stored_watchlists, watchlists):
            assert stored_watch.name == watch.name
            assert stored_watch.user_id == watch.user_id

    def test_create_watchlist_with_null_name(self, db, db_rollback):
        with pytest.raises(IntegrityError):
            watchlist = Watchlist(name=None, user_id=1)
            db.session.add(watchlist)
            db.session.commit()

    def test_create_watchlist_with_null_user_id(self, db, db_rollback):
        with pytest.raises(IntegrityError):
            watchlist = Watchlist(name="My Watchlist", user_id=None)
            db.session.add(watchlist)
            db.session.commit()


class TestWatchlistItem:

    def test_valid_parameters(self, db, db_teardown):
        item = WatchlistItem(
            ticker="AAPL",
            quantity=10,
            price=100.0,
            side="buy",
            trade_date=dt.date.today(),
            is_last_trade=False,
            created_timestamp=dt.datetime.utcnow(),
            comments="Test comment",
            watchlist_id=1
        )
        assert item.ticker == "AAPL"
        assert item.quantity == 10
        assert item.price == 100.0
        assert item.side == "buy"
        assert item.trade_date == dt.date.today()
        assert item.is_last_trade == False
        assert item.created_timestamp != None
        assert item.comments == "Test comment"
        assert item.watchlist_id == 1
        db.session.add(item)
        db.session.commit()
        stored_item = WatchlistItem.query.get(item.id)
        assert stored_item == item

    def test_invalid_ticker(self, db, db_rollback):
        with pytest.raises(Exception):
            item = WatchlistItem(
                ticker=None,
                quantity=10,
                price=100.0,
                side="buy",
                trade_date=dt.date.today(),
                watchlist_id=1
            )
            db.session.add(item)
            db.session.commit()

    def test_invalid_quantity(self, db, db_rollback):
        with pytest.raises(Exception):
            item = WatchlistItem(
                ticker="AAPL",
                quantity=None,
                price=100.0,
                side="buy",
                trade_date=dt.date.today(),
                watchlist_id=1
            )
            db.session.add(item)
            db.session.commit()

    def test_invalid_price(self, db, db_rollback):
        with pytest.raises(Exception):
            item = WatchlistItem(
                ticker="AAPL",
                quantity=10,
                price=None,
                side="buy",
                trade_date=dt.date.today(),
                watchlist_id=1
            )
            db.session.add(item)
            db.session.commit()

    def test_invalid_side(self, db, db_rollback):
        with pytest.raises(Exception):
            item = WatchlistItem(
                ticker="AAPL",
                quantity=10,
                price=170.0,
                side=None,
                trade_date=dt.date.today(),
                watchlist_id=1
            )
            db.session.add(item)
            db.session.commit()

    def test_invalid_trade_date(self, db, db_rollback):
        with pytest.raises(Exception):
            item = WatchlistItem(
                ticker="AAPL",
                quantity=10,
                price=170.0,
                side="buy",
                trade_date=None,
                watchlist_id=1
            )
            db.session.add(item)
            db.session.commit()


class TestGetSecurities:

    def test_returns_list_of_security_objects(self, db, db_teardown):
        securities = []
        security1 = Security(
            name="Security 1", 
            ticker="TICKER1", 
            exchange="EXCHANGE1", 
            currency="USD", 
            country="Country 1", 
            isin="ISIN1"
        )
        security2 = Security(
            name="Security 2", 
            ticker="TICKER2", 
            exchange="EXCHANGE2", 
            currency="EUR", 
            country="Country 2", 
            isin="ISIN2"
        )
        securities.extend([security1, security2])
        db.session.add_all(securities)
        db.session.commit()
        stored_securities = get_securities()
        assert isinstance(securities, list)
        assert len(stored_securities) == len(securities)
        for sec, stored_sec in zip(securities, stored_securities):
            assert isinstance(stored_sec, Security)
            assert stored_sec.name == sec.name
            assert stored_sec.ticker == sec.ticker
            assert stored_sec.exchange == sec.exchange
            assert stored_sec.currency == sec.currency
            assert stored_sec.country == sec.country
            assert stored_sec.isin == sec.isin

    def test_returns_empty_list(self):
        securities = get_securities()
        assert isinstance(securities, list)
        assert len(securities) == 0

    def test_returns_list_one_price_object(self, db, db_teardown):
        security = Security(
            name="Security 1", 
            ticker="TICKER1", 
            exchange="EXCHANGE1", 
            currency="USD", 
            country="Country 1", 
            isin="ISIN1"
        )
        db.session.add(security)
        db.session.commit()
        price = Price(date=dt.date(2022, 1, 1), close_price=10.0, ticker_id=security.id)
        db.session.add(price)
        db.session.commit()
        securities = get_securities()
        for sec in securities:
            assert len(sec.prices) == 1 # type: ignore
            assert isinstance(sec.prices[0], Price) # type: ignore
            assert sec.prices[0].date == dt.date(2022, 1, 1) # type: ignore
            assert sec.prices[0].close_price == 10.0 # type: ignore

    def test_returns_empty_list_no_price_objects(self, db, db_teardown):
        security = Security(
            name="Security 1", 
            ticker="TICKER1", 
            exchange="EXCHANGE1", 
            currency="USD", 
            country="Country 1", 
            isin="ISIN1"
        )
        db.session.add(security)
        db.session.commit()
        securities = get_securities()
        assert len(securities[0].prices) == 0 # type: ignore

