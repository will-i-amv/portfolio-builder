import datetime as dt

from flask import Blueprint, request, flash, redirect, render_template, url_for
from werkzeug.wrappers.response import Response
from flask_login import current_user, login_required

from portfolio_builder import db, scheduler
from portfolio_builder.public.forms import (
    AddWatchlistForm, SelectWatchlistForm, AddItemForm
)
from portfolio_builder.public.models import (
    Price, Security, Watchlist, WatchlistItem,
    get_all_watch_names, get_watch_items
)
from portfolio_builder.tasks import load_prices_ticker


bp = Blueprint("watchlist", __name__, url_prefix="/watchlist")


@bp.route("/", methods=['GET', 'POST'])
@login_required
def index() -> str:
    watch_names = get_all_watch_names()
    select_form = SelectWatchlistForm()
    add_watchlist_form = AddWatchlistForm()
    add_item_form = AddItemForm()
    select_form.watchlist.choices =  [
        (item, item)
        for item in watch_names
    ]
    if select_form.validate_on_submit():
        curr_watch_name = select_form.watchlist.data # Current watchlist name
    else:
        curr_watch_name = next(iter(watch_names), '')
    watch_items = get_watch_items(filter=[
        Watchlist.name == curr_watch_name,
        WatchlistItem.is_last_trade == True,
    ])
    securities = db.session.query(Security).all()
    return render_template(
        "public/watchlist.html", 
        select_form=select_form,
        add_watchlist_form=add_watchlist_form,
        add_item_form=add_item_form,
        curr_watch_name=curr_watch_name,
        securities=securities,
        watch_names=watch_names,
        watch_items=watch_items,
    )


@bp.route('/add_watchlist', methods=['POST'])
@login_required
def add_watchlist() -> Response:
    watchlist_form = AddWatchlistForm()
    if watchlist_form.validate_on_submit():
        watchlist_name = watchlist_form.name.data 
        new_watchlist = Watchlist(
            user_id=current_user.id, # type: ignore
            name=watchlist_name, 
        )
        db.session.add(new_watchlist)
        db.session.commit()
        flash(f"The watchlist '{watchlist_name}' has been added.")
    elif watchlist_form.errors:
        for error_name, error_desc in watchlist_form.errors.items():
            error_name = error_name.title()
            flash(f'{error_name}: {error_desc[0]}')
    return redirect(url_for('watchlist.index'))


@bp.route('/delete_watchlist', methods=['POST'])
@login_required
def delete_watchlist() -> Response:
    if not request.method == "POST":
        return redirect(url_for('watchlist.index'))
    else:
        watchlist_name = request.form.get('watchlist_group_removed')
        watchlist = (
            db
            .session
            .query(Watchlist)
            .filter(
                Watchlist.user_id==current_user.id, # type: ignore
                Watchlist.name==watchlist_name
            )
            .first()
        )
        if watchlist:
            db.session.delete(watchlist)
            db.session.commit()
            flash(f"The watchlist '{watchlist_name}' has been deleted.")
        return redirect(url_for('watchlist.index'))


@bp.route('/<watch_name>/add', methods=['POST'])
@login_required
def add(watch_name: str) -> Response:
    add_item_form = AddItemForm()
    if add_item_form.validate_on_submit():
        watchlist = (
            db
            .session
            .query(Watchlist)
            .filter(
                Watchlist.user_id==current_user.id, # type: ignore
                Watchlist.name==watch_name
            )
            .first()
        )    
        item = WatchlistItem(
            ticker=add_item_form.ticker.data, 
            quantity=add_item_form.quantity.data,
            price=add_item_form.price.data, 
            side=add_item_form.side.data,  
            trade_date=add_item_form.trade_date.data,
            comments=add_item_form.comments.data, 
            watchlist_id=watchlist.id
        )
        db.session.add(item)
        db.session.commit()
        flash(f"The ticker '{item.ticker}' has been added to the watchlist.")
        scheduler.add_job(
            id='add_db_last100day_prices',
            func=load_prices_ticker,
            args=[item.ticker],
        ) # task executes only once, immediately.
    elif add_item_form.errors:
        for error_name, error_desc in add_item_form.errors.items():
            error_name = error_name.title()
            flash(f"{error_name}: {error_desc[0]}")
    return redirect(url_for("watchlist.index"))


@bp.route('/<watch_name>/<ticker>/update', methods=['POST'])
@login_required
def update(watch_name: str, ticker: str) -> Response:
    add_item_form = AddItemForm()
    if add_item_form.validate_on_submit():
        last_item = get_watch_items(filter=[
            Watchlist.name == watch_name,
            WatchlistItem.ticker == ticker,
            WatchlistItem.is_last_trade == True,
        ])
        last_item = next(iter(last_item), '')
        if last_item:
            last_item.is_last_trade = False
            new_item = WatchlistItem(
                ticker=add_item_form.ticker.data, 
                quantity=add_item_form.quantity.data,
                price=add_item_form.price.data, 
                side=add_item_form.side.data, 
                trade_date=add_item_form.trade_date.data,
                comments=add_item_form.comments.data,
                watchlist_id=last_item.watchlist_id
            )
            db.session.add_all([last_item, new_item])
            db.session.commit()
            flash(f"The ticker '{new_item.ticker}' has been updated.")
    elif add_item_form.errors:
        for error_name, error_desc in add_item_form.errors.items():
            error_name = error_name.title()
            flash(f"{error_name}: {error_desc[0]}")
    return redirect(url_for("watchlist.index"))


@bp.route('/<watch_name>/<ticker>/delete', methods=['POST'])
@login_required
def delete(watch_name: str, ticker: str) -> Response:
    items = get_watch_items(filter=[
        Watchlist.name == watch_name,
        WatchlistItem.ticker == ticker,
    ])
    if items:
        for item in items:
            db.session.delete(item)
            db.session.commit()
        flash(f"The ticker '{ticker}' has been deleted from the watchlist.")
    return redirect(url_for('watchlist.index'))
