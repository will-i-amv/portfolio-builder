import datetime as dt
import pytest
from sqlalchemy.exc import IntegrityError, DataError

from portfolio_builder.public.models import (
    Security, Price, Watchlist, WatchlistItem, 
    get_securities, get_watchlists
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

    def test_invalid_ticker(self, db, with_rollback):
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

    def test_invalid_quantity(self, db, with_rollback):
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

    def test_invalid_price(self, db, with_rollback):
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

    def test_invalid_side(self, db, with_rollback):
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

    def test_invalid_trade_date(self, db, with_rollback):
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

