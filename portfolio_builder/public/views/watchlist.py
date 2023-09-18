from flask_login import current_user
from flask import Blueprint, request, flash, redirect, render_template, url_for
from flask_login import login_required
from sqlalchemy.orm import aliased

from portfolio_builder import db
from portfolio_builder.public.forms import (
    WatchlistAddForm, WatchlistSelectForm, WatchlistAddItemForm
)
from portfolio_builder.public.models import Watchlist, WatchlistItem


bp = Blueprint("watchlist", __name__, url_prefix="/watchlist")


def get_watchlist_names():
    watchlists = (
        db
        .session
        .query(Watchlist)
        .with_entities(Watchlist.name)
        .filter_by(user_id=current_user.id)
        .order_by(Watchlist.id)
        .all()
    )
    return [item[0] for item in watchlists]

def get_watchlist_items(name):
    watch_itm = aliased(WatchlistItem)
    watch = aliased(Watchlist)
    watchlist_items = (
        db
        .session
        .query(watch_itm)
        .join(watch, onclause=(watch_itm.watchlist_id==watch.id))
        .filter(
            watch.user_id == current_user.id,
            watch.name == name,
        )
        .with_entities(
            watch_itm.id,
            watch_itm.ticker,
            watch_itm.quantity,
            watch_itm.price,
            watch_itm.sector,
            watch_itm.trade_date,
            watch_itm.created_timestamp,
            watch_itm.comments,
        )
        .all()
    )
    return watchlist_items


@bp.route("/", methods=('GET', 'POST'))
@login_required
def index():
    watch_names = get_watchlist_names()
    select_form = WatchlistSelectForm()
    add_watchlist_form = WatchlistAddForm()
    add_item_form = WatchlistAddItemForm()
    select_form.watchlist.choices =  [
        (item, item)
        for item in watch_names
    ]
    add_item_form.watchlist.choices =  [
        (item, item)
        for item in watch_names
    ]
    if select_form.validate_on_submit():
        curr_watch_name = select_form.watchlist.data # Current watchlist name
    else:
        curr_watch_name = next(iter(watch_names), '')
    watch_items = get_watchlist_items(curr_watch_name)
    return render_template(
        "public/watchlist.html", 
        select_form=select_form,
        add_watchlist_form=add_watchlist_form,
        add_item_form=add_item_form,
        curr_watch_name=curr_watch_name,
        watch_names=watch_names,
        watch_items=watch_items,
    )


@bp.route('/add', methods=['POST'])
@login_required
def add_watchlist():
    watchlist_form = WatchlistAddForm()
    if watchlist_form.validate_on_submit():
        watchlist_name = watchlist_form.name.data 
        new_watchlist = Watchlist(
            name=watchlist_name, 
            user_id=current_user.id
        )
        db.session.add(new_watchlist)
        db.session.commit()
        flash(f"The Watchlist group '{watchlist_name}' has been created")
        return redirect(url_for('watchlist.index'))
    elif watchlist_form.errors:
        for error_name, error_desc in watchlist_form.errors.items():
            error_name = error_name.title()
            flash(f'{error_name}: {error_desc[0]}')
    return redirect(url_for('watchlist.index'))


@bp.route('/delete', methods=('POST',))
@login_required
def delete_watchlist():
    if not request.method == "POST":
        return redirect(url_for('watchlist.index'))
    else:
        watchlist_name = request.form.get('watchlist_group_removed')
        watchlist = (
            db
            .session
            .query(Watchlist)
            .filter(
                Watchlist.user_id==current_user.id,
                Watchlist.name==watchlist_name
            )
            .first()
        )
        db.session.delete(watchlist)
        db.session.commit()
        return redirect(url_for('watchlist.index'))


@bp.route('/add_item', methods=['POST'])
@login_required
def add_item():
    watchlists = get_watchlist_names()
    form = WatchlistAddItemForm()
    form.watchlist.choices = [
        (item, item)
        for item in watchlists
    ]
    if form.validate_on_submit():
        watchlist_name = form.watchlist.data
        ticker = form.ticker.data
        watchlist = (
            db
            .session
            .query(Watchlist)
            .filter(
                Watchlist.user_id==current_user.id,
                Watchlist.name==watchlist_name
            )
            .first()
        )    
        new_item = WatchlistItem(
            watchlist=watchlist_name, 
            ticker=ticker, 
            quantity=form.quantity.data,
            price=form.price.data, 
            sector=form.sector.data,
            comments=form.comments.data, 
            watchlist_id=watchlist.id
        )
        db.session.add(new_item)
        db.session.commit()
        flash(f"{ticker} has been added to your watchlist")
        return redirect(url_for("watchlist.index"))
    elif form.errors:
        for error_name, error_desc in form.errors.items():
            error_name = error_name.title()
            flash(f"{error_name}: {error_desc[0]}")
    return redirect(url_for("watchlist.index"))


@bp.route('/<int:id>/<watch_name>/update_item', methods=['POST'])
@login_required
def update_item(id, watch_name):
    watchlists = get_watchlist_names()
    add_item_form = WatchlistAddItemForm()
    add_item_form.watchlist.choices =  [
        (item, item)
        for item in watchlists
    ]
    if add_item_form.validate_on_submit():
        watch_itm = aliased(WatchlistItem)
        watch = aliased(Watchlist)
        item = (
            db
            .session
            .query(watch_itm)
            .join(watch, onclause=(watch_itm.watchlist_id==watch.id))
            .filter(
                watch.user_id == current_user.id,
                watch.name == watch_name,
                watch_itm.id==id,
            )
            .first()
        )
        if item:
            item.ticker = add_item_form.ticker.data
            item.watchlist = add_item_form.watchlist.data
            item.quantity = add_item_form.quantity.data
            item.price = add_item_form.price.data
            item.trade_date = add_item_form.trade_date.data
            item.sector = add_item_form.sector.data
            item.comments = add_item_form.comments.data
            db.session.add(item)
            db.session.commit()
            flash(f"Item with ticker {item.ticker} has been updated")
    elif add_item_form.errors:
        for error_name, error_desc in add_item_form.errors.items():
            error_name = error_name.title()
            flash(f"{error_name}: {error_desc[0]}")
    return redirect(url_for("watchlist.index"))


@bp.route('/<int:id>/<watch_name>/delete_item', methods=['POST'])
@login_required
def delete_item(id, watch_name):
    watch_itm = aliased(WatchlistItem)
    watch = aliased(Watchlist)
    item = (
        db
        .session
        .query(watch_itm)
        .join(watch, onclause=(watch_itm.watchlist_id==watch.id))
        .filter(
            watch.user_id == current_user.id,
            watch.name == watch_name,
            watch_itm.id==id,
        )
        .first()
    )
    if item:
        db.session.delete(item)
        db.session.commit()
    return redirect(url_for('watchlist.index'))
