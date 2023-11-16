import datetime as dt

import pytest
from portfolio_builder.auth.models import User
from portfolio_builder.public.models import (
    Watchlist, WatchlistItem, Security,
    WatchlistItemMgr
)
from portfolio_builder.public.tasks import load_securities_csv


def _get_messages(sesssion):
    return [msg[1] for msg in sesssion['_flashes']]


@pytest.fixture(scope='module')
def all_tickers(db):
    load_securities_csv()
    tickers = db.session.query(Security).with_entities(Security.ticker).all()
    yield [item.ticker for item in tickers]
    _ = db.session.query(Security).delete()
    db.session.commit()


class TestAdd:
    @pytest.fixture(scope='function')
    def loaded_db(self, db):
        user = db.session.query(User).first()
        watchlist = Watchlist(name="Test_Watchlist", user_id=user.id)
        db.session.add(watchlist)
        db.session.commit()
        yield
        db.session.query(WatchlistItem).delete()
        db.session.query(Watchlist).delete()
        db.session.commit()

    @pytest.mark.usefixtures("login_required")
    def test_add_item(self, client, loaded_db, all_tickers):
        # Add a new item to a watchlist
        ticker = 'AAPL'
        watch_name = 'Test_Watchlist'
        response = client.post(
            f'/watchlist/{watch_name}/add', 
            data={
                'ticker': ticker,
                'quantity': 15,
                'price': 175.0,
                'side': 'buy',
                'trade_date': dt.date.today(),
                'comments': ''
            },
        )
        assert response.status_code == 302
        assert len(WatchlistItemMgr.get_items(filters=[
            Watchlist.user_id==1, 
            Watchlist.name==watch_name, 
            WatchlistItem.ticker==ticker])
        ) == 1
        with client.session_transaction() as session:
            messages = _get_messages(session)
            assert ticker in messages[0]
            assert "has been added to the watchlist" in messages[0]

    @pytest.mark.usefixtures("login_required")
    def test_add_item_to_nonexistent_watchlist(self, client, db, loaded_db, all_tickers):
        # Add an item to a non-existent watchlist
        ticker = 'MSFT'
        watch_name = 'Non_existing_watchlist'
        response = client.post(f'/watchlist/{watch_name}/add', data={
            'ticker': ticker,
            'quantity': 10,
            'price': 330.0,
            'side': 'buy',
            'trade_date': dt.date.today(),
            'comments': ''
        })
        assert response.status_code == 302
        with client.session_transaction() as session:
            messages = _get_messages(session)
            assert ticker not in messages[0]
            assert "does not exist" in messages[0]


class TestDelete:
    @pytest.fixture(scope='function')
    def loaded_db(self, db):
        user = db.session.query(User).first()
        watchlist = Watchlist(name="Test_Watchlist", user_id=user.id)
        db.session.add(watchlist)
        db.session.commit()
        item1 = WatchlistItem(
            ticker='AAPL',
            quantity=10,
            price=175.0,
            side='buy',
            trade_date=dt.date.today(),
            watchlist_id=watchlist.id,
        )
        item2 = WatchlistItem(
            ticker='AMZN',
            quantity=7,
            price=130.0,
            side='sell',
            trade_date=dt.date.today(),
            watchlist_id=watchlist.id,
        )
        item3 = WatchlistItem(
            ticker='MSFT',
            quantity=5,
            price=330.0,
            side='sell',
            trade_date=dt.date.today(),
            watchlist_id=watchlist.id,
        )
        db.session.add_all([item1, item2, item3])
        db.session.commit()
        yield
        db.session.query(WatchlistItem).delete()
        db.session.query(Watchlist).delete()
        db.session.commit()

    @pytest.mark.usefixtures("login_required")
    def test_delete_single_ticker(self, client, loaded_db):
        # Deletes a specific ticker from a watchlist.
        ticker = 'AAPL'
        watch_name = "Test_Watchlist"
        assert len(WatchlistItemMgr.get_items(filters=[
            Watchlist.user_id==1, 
            Watchlist.name==watch_name, 
            WatchlistItem.ticker==ticker])
        ) == 1
        response = client.post(f'/watchlist/{watch_name}/{ticker}/delete')
        assert response.status_code == 302
        assert len(WatchlistItemMgr.get_items(filters=[
            Watchlist.user_id==1, 
            Watchlist.name==watch_name, 
            WatchlistItem.ticker==ticker])
        ) == 0
        with client.session_transaction() as session:
            messages = _get_messages(session)
            assert ticker in messages[0]
            assert 'have been deleted' in messages[0]


    @pytest.mark.usefixtures("login_required")
    def test_delete_multiple_tickers(self, client, loaded_db):
        # Deletes multiple tickers from a watchlist.
        ticker1 = 'AAPL'
        ticker2 = 'AMZN'
        watch_name = "Test_Watchlist"
        assert len(WatchlistItemMgr.get_items(filters=[
            Watchlist.user_id==1, 
            Watchlist.name==watch_name, 
            WatchlistItem.ticker.in_([ticker1, ticker2])
        ])) == 2
        response = client.post(f'/watchlist/{watch_name}/{ticker1}/delete')
        response = client.post(f'/watchlist/{watch_name}/{ticker2}/delete')
        assert response.status_code == 302
        assert len(WatchlistItemMgr.get_items(filters=[
            Watchlist.user_id==1, 
            Watchlist.name==watch_name, 
            WatchlistItem.ticker.in_([ticker1, ticker2])
        ])) == 0
        with client.session_transaction() as session:
            messages = _get_messages(session)
            assert all([('have been deleted' in msg) for msg in messages])
            assert any([(ticker1 in msg) for msg in messages])
            assert any([(ticker2 in msg) for msg in messages])

    @pytest.mark.usefixtures("login_required")
    def test_delete_nonexistent_ticker(self, client, loaded_db):
        ticker = 'GOOG'
        watch_name = "Test_Watchlist"
        assert len(WatchlistItemMgr.get_items(filters=[
            Watchlist.user_id==1, 
            Watchlist.name==watch_name, 
            WatchlistItem.ticker==ticker])
        ) == 0
        response = client.post(f'/watchlist/{watch_name}/{ticker}/delete')
        assert len(WatchlistItemMgr.get_items(filters=[
            Watchlist.user_id==1, 
            Watchlist.name==watch_name, 
            WatchlistItem.ticker==ticker])
        ) == 0
        assert response.status_code == 302
        with client.session_transaction() as session:
            messages = _get_messages(session)
            assert 'An error occurred' in messages[0]

    @pytest.mark.usefixtures("login_required")
    def test_delete_from_nonexistent_watchlist(self, client, loaded_db):
        ticker = 'AAPL'
        watch_name = "Nonexising_Watchlist"
        response = client.post(f'/watchlist/{watch_name}/{ticker}/delete')
        assert response.status_code == 302
        with client.session_transaction() as session:
            messages = _get_messages(session)
            assert 'An error occurred' in messages[0]

    def test_delete_ticker_unauthenticated(self, client, loaded_db):
        ticker = 'AAPL'
        watch_name = "Test_Watchlist"
        assert len(WatchlistItemMgr.get_items(filters=[
            Watchlist.user_id==1, 
            Watchlist.name==watch_name, 
            WatchlistItem.ticker==ticker])
        ) == 1
        response = client.post(f'/watchlist/{watch_name}/{ticker}/delete')
        assert response.status_code == 302
        assert len(WatchlistItemMgr.get_items(filters=[
            Watchlist.user_id==1, 
            Watchlist.name==watch_name, 
            WatchlistItem.ticker==ticker])
        ) == 1
        with client.session_transaction() as session:
            messages = _get_messages(session)
            assert 'Please log in' in messages[0]
