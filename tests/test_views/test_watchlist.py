import datetime as dt

import pytest
from portfolio_builder.auth.models import User
from portfolio_builder.public.models import Watchlist, WatchlistItem, get_watch_items


def _get_messages(sesssion):
    return [msg[1] for msg in sesssion['_flashes']]


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
        db.session.delete(item1)
        db.session.delete(item2)
        db.session.delete(item3)
        db.session.commit()

    @pytest.mark.usefixtures("login_required")
    def test_delete_ticker_from_watchlist(self, client, loaded_db):
        # Deletes a specific ticker from a watchlist.
        ticker = 'AAPL'
        watch_name = "Test_Watchlist"
        assert len(get_watch_items(filter=[
            Watchlist.user_id==1, 
            Watchlist.name==watch_name, 
            WatchlistItem.ticker==ticker])
        ) == 1
        response = client.post(f'/watchlist/Test_Watchlist/{ticker}/delete')
        assert response.status_code == 302
        assert len(get_watch_items(filter=[
            Watchlist.user_id==1, 
            Watchlist.name==watch_name, 
            WatchlistItem.ticker==ticker])
        ) == 0
        with client.session_transaction() as session:
            messages = _get_messages(session)
            assert ticker in messages[0]
            assert 'have been deleted' in messages[0]


    @pytest.mark.usefixtures("login_required")
    def test_delete_multiple_tickers_from_watchlist(self, client, loaded_db):
        # Deletes multiple tickers from a watchlist.
        ticker1 = 'AAPL'
        ticker2 = 'AMZN'
        watch_name = "Test_Watchlist"
        assert len(get_watch_items(filter=[
            Watchlist.user_id==1, 
            Watchlist.name==watch_name, 
            WatchlistItem.ticker.in_([ticker1, ticker2])
        ])) == 2
        response = client.post(f'/watchlist/Test_Watchlist/{ticker1}/delete')
        response = client.post(f'/watchlist/Test_Watchlist/{ticker2}/delete')
        assert response.status_code == 302
        assert len(get_watch_items(filter=[
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
    def test_delete_ticker_from_watchlist_with_multiple_tickers(self, client, loaded_db):
        # Deletes a ticker from a watchlist with multiple tickers.
        ticker1 = 'AAPL'
        ticker2 = 'MSFT'
        watch_name = "Test_Watchlist"
        response = client.post('/watchlist/Test_Watchlist/AAPL/delete')
        assert response.status_code == 302
        assert len(get_watch_items(filter=[
            Watchlist.user_id==1, 
            Watchlist.name==watch_name, 
            WatchlistItem.ticker==ticker1])
        ) == 0
        assert len(get_watch_items(filter=[
            Watchlist.user_id==1, 
            Watchlist.name=="Test_Watchlist", 
            WatchlistItem.ticker==ticker2])
        ) == 1
        with client.session_transaction() as session:
            messages = _get_messages(session)
            assert 'have been deleted' in messages[0]
            assert ticker1 in messages[0]
            assert ticker2 not in messages[0]

    @pytest.mark.usefixtures("login_required")
    def test_delete_nonexistent_ticker_from_watchlist(self, client, loaded_db):
        ticker = 'GOOG'
        watch_name = "Test_Watchlist"
        response = client.post('/watchlist/Test_Watchlist/{ticker}/delete')
        assert response.status_code == 302
        assert len(get_watch_items(filter=[
            Watchlist.user_id==1, 
            Watchlist.name==watch_name, 
            WatchlistItem.ticker==ticker])
        ) == 0
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

    def test_delete_ticker_from_watchlist_unauthenticated(self, client, loaded_db):
        ticker = 'AAPL'
        watch_name = "Test_Watchlist"
        response = client.post(f'/watchlist/{watch_name}/{ticker}/delete')
        assert response.status_code == 302
        assert len(get_watch_items(filter=[
            Watchlist.user_id==1, 
            Watchlist.name==watch_name, 
            WatchlistItem.ticker==ticker])
        ) == 1
        with client.session_transaction() as session:
            messages = _get_messages(session)
            assert 'Please log in' in messages[0]
