import datetime as dt
import random

import pytest
from sqlalchemy.engine.row import Row
from sqlalchemy.exc import IntegrityError

from portfolio_builder.public.models import (
    Security, Price, Watchlist, WatchlistItem,  get_securities, 
    get_first_watchlist, get_watchlists, get_first_watch_item, get_watch_items
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
def watchlists(db):
    watchlists = [
        Watchlist(name="Technology", user_id=1),
        Watchlist(name="Real Estate", user_id=1),
        Watchlist(name="Oil and Gas", user_id=1),
    ]
    db.session.add_all(watchlists)
    db.session.commit()
    yield watchlists


@pytest.fixture(scope='function')
def watch_items(db, watchlists):
    watchlist = random.choice(watchlists)
    items = [
        WatchlistItem(
            ticker = 'AAPL',
            quantity = 10,
            price = 170.0,
            side = 'buy',
            trade_date = dt.date(2023, 10, 10),
            watchlist_id=watchlist.id,
        ),
        WatchlistItem(
            ticker = 'AAPL',
            quantity = 5,
            price = 168.0,
            side = 'buy',
            trade_date = dt.date(2023, 10, 11),
            watchlist_id=watchlist.id,
        ),
        WatchlistItem(
            ticker = 'AAPL',
            quantity = 5,
            price = 173.0,
            side = 'sell',
            trade_date = dt.date(2023, 10, 12),
            watchlist_id=watchlist.id,
            is_last_trade=True,
        ),
        WatchlistItem(
            ticker = 'AMZN',
            quantity = 15,
            price = 130.0,
            side = 'buy',
            trade_date = dt.date(2023, 10, 10),
            watchlist_id=watchlist.id,
        ),
        WatchlistItem(
            ticker = 'AMZN',
            quantity = 3,
            price = 132.0,
            side = 'sell',
            trade_date = dt.date(2023, 10, 11),
            watchlist_id=watchlist.id,
        ),
        WatchlistItem(
            ticker = 'AMZN',
            quantity = 5,
            price = 125.0,
            side = 'buy',
            trade_date = dt.date(2023, 10, 12),
            watchlist_id=watchlist.id,
            is_last_trade=True,
        ),
        WatchlistItem(
            ticker = 'MSFT',
            quantity = 6,
            price = 330.0,
            side = 'buy',
            trade_date = dt.date(2023, 10, 10),
            watchlist_id=watchlist.id,
        ),
        WatchlistItem(
            ticker = 'MSFT',
            quantity = 1,
            price = 335.0,
            side = 'sell',
            trade_date = dt.date(2023, 10, 11),
            watchlist_id=watchlist.id,
        ),
        WatchlistItem(
            ticker = 'MSFT',
            quantity = 3,
            price = 338.0,
            side = 'sell',
            trade_date = dt.date(2023, 10, 12),
            watchlist_id=watchlist.id,
            is_last_trade=True,
        ),
    ]
    db.session.add_all(items)
    db.session.commit()
    yield items


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
    def test_create_rows(self, db, db_teardown, securities):
        new_securities = [
            Security(name="Alphabet Inc.", ticker="GOOG", exchange="NASDAQ"),
            Security(name="Meta Platforms Inc.", ticker="META", exchange="NASDAQ")
        ]
        len_sec_before = len(securities)
        db.session.add_all(new_securities)
        db.session.commit()
        securities_after = db.session.query(Security).all()
        len_sec_after = len(securities_after)
        assert len_sec_after - len_sec_before == len(new_securities)

    def test_get_row(self, db, db_teardown, securities):
        security = random.choice(securities)
        stored_security = db.session.query(Security).filter_by(ticker=security.ticker).first()
        assert stored_security is not None
        assert stored_security.name == security.name
        assert stored_security.exchange == security.exchange

    def test_get_all_rows(self, db, db_teardown, securities):
        all_securities = db.session.query(Security).all()
        assert len(all_securities) == len(securities)

    def test_update_row(self, db, db_teardown, securities):
        security = random.choice(securities)
        security.name = "New Name"
        security.ticker = "NEW"
        security.exchange = "NYSE"
        db.session.commit()
        updated_security = db.session.query(Security).filter_by(id=security.id).first()
        assert updated_security.name == security.name
        assert updated_security.ticker == security.ticker
        assert updated_security.exchange == security.exchange

    def test_delete_row(self, db, db_teardown, securities):
        len_sec_before = len(securities)
        security_to_delete = random.choice(securities)
        db.session.delete(security_to_delete)
        db.session.commit()
        securities_after = db.session.query(Security).all()
        len_sec_after = len(securities_after)
        assert len_sec_before - len_sec_after == 1

    def test_create_rows_null_mandatory_fields(self, db, db_teardown):
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
        assert price.ticker_id == sec.id
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
            filters=[Watchlist.user_id == user_id],
            entities=[Watchlist.name, Watchlist.user_id]
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


class TestGetWatchlists:

    def test_matching_filter(self, watchlists):
        watchlist = random.choice(watchlists)
        result = get_watchlists(filters=[Watchlist.name == watchlist.name])
        assert isinstance(result, list)
        assert len(result) > 0
    
    def test_not_matching_filters(self, db):
        result = get_watchlists(filters=[Watchlist.name == 'Nonexistent Watchlist'])
        assert isinstance(result, list)
        assert len(result) == 0

    # Returns an empty list when no watchlists exist in the database
    def test_empty_watchlists_table(self, db):
        db.session.query(Watchlist).delete()
        db.session.commit()
        result = get_watchlists([])
        assert isinstance(result, list)
        assert len(result) == 0

    def test_default_entities_param(self):
        result = get_watchlists(filters=[])
        assert all(isinstance(item, Row) for item in result)
        assert all(isinstance(item.name, str) for item in result)

    def test_valid_entities_param(self):
        result = get_watchlists(filters=[], entities=[Watchlist.id, Watchlist.user_id])
        assert all(isinstance(item, Row) for item in result)
        assert all(isinstance(item.id, int) for item in result)
        assert all(isinstance(item.user_id, int) for item in result)

    def test_default_orderby_param(self):
        result = [
            item.id
            for item in get_watchlists(filters=[], entities=[Watchlist.id])
        ]
        assert all(
            result[i] <= result[i+1] 
            for i in range(len(result)-1)
        )

    def test_orderby_asc(self):
        result = [
            item.name
            for item in get_watchlists(filters=[], orderby=[Watchlist.name])
        ]
        assert all(
            result[i] <= result[i+1] 
            for i in range(len(result)-1)
        )

    def test_orderby_desc(self):
        result = [
            item.name
            for item in get_watchlists(filters=[], orderby=[Watchlist.name.desc()])
        ]
        assert all(
            result[i] >= result[i+1] 
            for i in range(len(result)-1)
        )

    def test_raises_error_invalid_filters_param(self):
        with pytest.raises(AttributeError):
            get_watchlists(filters=[Watchlist.invalid_column == 0])

    def test_raises_error_invalid_entities_param(self):
        with pytest.raises(AttributeError):
            get_watchlists(filters=[], entities=[Watchlist.invalid_column])

    # Raises an error when given an invalid orderby parameter
    def test_raises_error_invalid_orderby_param(self):
        with pytest.raises(Exception):
            get_watchlists(filters=[], orderby=[Watchlist.invalid_column])


class TestGetWatchItems:

    def test_returns_match_filter(self, watch_items, db_teardown):
        # Returns a list of rows containing the selected columns 
        # from WatchlistItem table, ordered by the given orderby parameter.
        watch_item = random.choice(watch_items)
        result = get_watch_items(filters=[WatchlistItem.ticker == watch_item.ticker])
        assert len(result) > 0
        assert isinstance(result, list)
        assert all(isinstance(item, Row) for item in result)
        assert all(isinstance(item.id, int) for item in result)
        assert all(isinstance(item.ticker, str) for item in result)
        assert all(isinstance(item.quantity, int) for item in result)
        assert all(isinstance(item.price, float) for item in result)
        assert all(isinstance(item.side, str) for item in result)
        assert all(isinstance(item.trade_date, dt.date) for item in result)
        assert all(item.ticker == watch_item.ticker for item in result)

    def test_returns_match_all_filter(self, watch_items, db_teardown):
        result = get_watch_items(filters=[])
        assert len(result) == len(watch_items)
    
    def test_returns_no_match_filter(self, db):
        # Returns an empty list if no rows match the given filter.
        result = get_watch_items(filters=[
            WatchlistItem.ticker == 'Nonexistent Ticker'
        ])
        assert isinstance(result, list)
        assert len(result) == 0

    def test_returns_selected_columns(self, watch_items):
        # Returns only the columns specified in the select parameter.
        watch_item = random.choice(watch_items)
        filters = [WatchlistItem.ticker == watch_item.ticker]
        select = [WatchlistItem.id, WatchlistItem.ticker]
        result = get_watch_items(filters=filters, entities=select)
        assert all((len(row) == len(select)) for row in result)
        assert all(isinstance(row.id, int) for row in result)
        assert all(isinstance(row.ticker, str) for row in result)

    def test_returns_rows_ascending_order(self, db):
        # Returns rows in ascending order by default.
        result = get_watch_items(filters=[WatchlistItem.ticker == 'AAPL'])
        assert all(
            result[i].id <= result[i+1].id 
            for i in range(len(result)-1)
        )

    def test_returns_rows_descending_order(self, db):
        # Returns rows in descending order if orderby parameter 
        # is given with a descending order.
        filter = [WatchlistItem.ticker == 'AAPL']
        orderby = [WatchlistItem.id.desc()]
        result = get_watch_items(filters=filter, orderby=orderby)
        assert all(
            result[i].id >= result[i+1].id 
            for i in range(len(result)-1)
        )

    def test_returns_empty_list_invalid_filter(self, db, db_rollback):
        # Raises an error if the filter parameter is invalid.
        with pytest.raises(AttributeError):
            get_watch_items(filters=[WatchlistItem.invalid_column == 0])

    def test_returns_empty_list_invalid_select(self, watch_items, db_rollback):
        # Raises an error if the select parameter is invalid.
        watch_item = random.choice(watch_items)
        with pytest.raises(AttributeError):
            get_watch_items(
                filters=[WatchlistItem.ticker == watch_item.ticker], 
                entities=[WatchlistItem.invalid_column]
            )

    def test_returns_empty_list_invalid_orderby(self, watch_items, db_rollback):
        # Raises an error if the orderby parameter is invalid.
        watch_item = random.choice(watch_items)
        with pytest.raises(AttributeError):
            get_watch_items(
                filters=[WatchlistItem.ticker == watch_item.ticker], 
                orderby = [WatchlistItem.invalid_column],
            )


class TestGetFirstWatchlist:

    def test_returns_first_valid_filter(self, watchlists, db_teardown):
        # Returns first Watchlist object when a valid filter is provided
        watchlist = random.choice(watchlists)
        result = get_first_watchlist(filters=[Watchlist.name == watchlist.name])
        assert isinstance(result, Watchlist)

    def test_returns_none_no_match(self, db_teardown):
        # Returns None when no Watchlist matches the filter
        result = get_first_watchlist(filters=[Watchlist.name == "Nonexistent Watchlist"])
        assert result is None

    def test_returns_first_multiple_match(self, watchlists):
        # Returns first Watchlist object that matches the filter 
        # that multiple Watchlists match.
        watchlist = random.choice(watchlists)
        result = get_first_watchlist(filters=[Watchlist.name.like(f"{watchlist.name[:4]}%")])
        assert isinstance(result, Watchlist)

    def test_returns_first_all_filter(self, db_teardown):
        # Returns one Watchlist object when a 'match_all' filter is provided
        result = get_first_watchlist(filters=[])
        assert result is not None
        assert isinstance(result, Watchlist)

    def test_raise_error_invalid_filter(self, db_rollback):
        # Raises an error when an invalid filter is provided
        with pytest.raises(AttributeError):
            get_first_watchlist(filters=[Watchlist.invalid_column == "Invalid"])

