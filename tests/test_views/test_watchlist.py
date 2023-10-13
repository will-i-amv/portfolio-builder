import datetime as dt

import pytest
from portfolio_builder.public.models import Watchlist, WatchlistItem, get_watch_items


class TestDeleteItems:
    @pytest.mark.usefixtures("authenticated_request")
    def test_delete_ticker_from_watchlist(self, client, db):
        # Deletes a specific ticker from a watchlist.
        watchlist = Watchlist(name="Test_Watchlist", user_id=1)
        db.session.add(watchlist)
        db.session.commit()
        watchlist_item = WatchlistItem(
            ticker='AAPL',
            quantity=10,
            price=175.0,
            side='buy',
            trade_date=dt.date.today(),
            watchlist_id=watchlist.id,
        )
        db.session.add(watchlist_item)
        db.session.commit()
        response = client.post('/watchlist/Test_Watchlist/AAPL/delete')
        assert response.status_code == 302
        assert len(get_watch_items(filter=[
            Watchlist.user_id==1, 
            Watchlist.name=="Test_Watchlist", 
            WatchlistItem.ticker=="AAPL"])
        ) == 0
        with client.session_transaction() as session:
            message = str(session['_flashes'])
            assert watchlist_item.ticker in message
            assert 'has been deleted' in message

    @pytest.mark.usefixtures("authenticated_request")
    def test_delete_multiple_tickers_from_watchlist(self, client, db):
        # Deletes multiple tickers from a watchlist.
        watchlist = Watchlist(name="Test_Watchlist", user_id=1)
        db.session.add(watchlist)
        db.session.commit()
        watchlist_item1 = WatchlistItem(
            ticker='AMZN',
            quantity=10,
            price=131.0,
            side='buy',
            trade_date=dt.date.today(),
            watchlist_id=watchlist.id,
        )
        watchlist_item2 = WatchlistItem(
            ticker='MSFT',
            quantity=5,
            price=330,
            side='sell',
            trade_date=dt.date.today(),
            watchlist_id=watchlist.id,
        )
        db.session.add_all([watchlist_item1, watchlist_item2])
        db.session.commit()
        response = client.post('/watchlist/Test_Watchlist/AMZN/delete')
        response = client.post('/watchlist/Test_Watchlist/MSFT/delete')
        assert response.status_code == 302
        assert len(get_watch_items(filter=[
            Watchlist.user_id==1, 
            Watchlist.name=="Test_Watchlist", 
            WatchlistItem.ticker=="AMZN"])
        ) == 0
        assert len(get_watch_items(filter=[
            Watchlist.user_id==1, 
            Watchlist.name=="Test_Watchlist", 
            WatchlistItem.ticker=="MSFT"])
        ) == 0
        with client.session_transaction() as session:
            all_messages = str(session['_flashes'])
            assert watchlist_item1.ticker in all_messages
            assert watchlist_item2.ticker in all_messages
            assert 'has been deleted' in all_messages

    @pytest.mark.usefixtures("authenticated_request")
    def test_delete_ticker_from_watchlist_with_multiple_tickers(self, client, db):
        # Deletes a ticker from a watchlist with multiple tickers.
        watchlist = Watchlist(name="Test_Watchlist", user_id=1)
        db.session.add(watchlist)
        db.session.commit()
        watchlist_item1 = WatchlistItem(
            ticker='AAPL',
            quantity=7,
            price=182.0,
            side='buy',
            trade_date=dt.date.today(),
            watchlist_id=watchlist.id,
        )
        watchlist_item2 = WatchlistItem(
            ticker='MSFT',
            quantity=5,
            price=330,
            side='sell',
            trade_date=dt.date.today(),
            watchlist_id=watchlist.id,
        )
        db.session.add_all([watchlist_item1, watchlist_item2])
        db.session.commit()
        response = client.post('/watchlist/Test_Watchlist/AAPL/delete')
        assert response.status_code == 302
        assert len(get_watch_items(filter=[
            Watchlist.user_id==1, 
            Watchlist.name=="Test_Watchlist", 
            WatchlistItem.ticker=="AAPL"])
        ) == 0
        assert len(get_watch_items(filter=[
            Watchlist.user_id==1, 
            Watchlist.name=="Test_Watchlist", 
            WatchlistItem.ticker=="MSFT"])
        ) == 1
        with client.session_transaction() as session:
            message = str(session['_flashes'])
            assert watchlist_item1.ticker in message
            assert watchlist_item2.ticker not in message
            assert 'has been deleted' in message
