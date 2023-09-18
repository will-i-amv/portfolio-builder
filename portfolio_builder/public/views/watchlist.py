from flask_login import current_user
from flask import Blueprint, request, flash, redirect, render_template, url_for
from flask_login import login_required
from sqlalchemy.orm import aliased

from portfolio_builder import db
from portfolio_builder.public.forms import (
    AddWatchlistForm, SelectWatchlistForm, AddItemForm
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


def get_watchlist_items(watch_name):
    watch_itm = aliased(WatchlistItem)
    watch = aliased(Watchlist)
    watchlist_items = (
        db
        .session
        .query(watch_itm)
        .join(watch, onclause=(watch_itm.watchlist_id==watch.id))
        .filter(
            watch.user_id == current_user.id,
            watch.name == watch_name,
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


def get_watchlist_item(id, watch_name):
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
    return item


@bp.route("/", methods=['GET', 'POST'])
@login_required
def index():
    watch_names = get_watchlist_names()
    select_form = SelectWatchlistForm()
    add_watchlist_form = AddWatchlistForm()
    add_item_form = AddItemForm()
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


@bp.route('/add_watchlist', methods=['POST'])
@login_required
def add_watchlist():
    watchlist_form = AddWatchlistForm()
    if watchlist_form.validate_on_submit():
        watchlist_name = watchlist_form.name.data 
        new_watchlist = Watchlist(
            name=watchlist_name, 
            user_id=current_user.id
        )
        db.session.add(new_watchlist)
        db.session.commit()
        flash(f"The watchlist '{watchlist_name}' has been added.")
        return redirect(url_for('watchlist.index'))
    elif watchlist_form.errors:
        for error_name, error_desc in watchlist_form.errors.items():
            error_name = error_name.title()
            flash(f'{error_name}: {error_desc[0]}')
    return redirect(url_for('watchlist.index'))


@bp.route('/delete_watchlist', methods=['POST'])
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
        if watchlist:
            db.session.delete(watchlist)
            db.session.commit()
            flash(f"The watchlist '{watchlist_name}' has been deleted.")
        return redirect(url_for('watchlist.index'))


@bp.route('/add', methods=['POST'])
@login_required
def add():
    watchlists = get_watchlist_names()
    add_item_form = AddItemForm()
    add_item_form.watchlist.choices = [
        (item, item)
        for item in watchlists
    ]
    if add_item_form.validate_on_submit():
        watchlist_name = add_item_form.watchlist.data
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
        item = WatchlistItem(
            watchlist=watchlist_name, 
            ticker=add_item_form.ticker.data, 
            quantity=add_item_form.quantity.data,
            price=add_item_form.price.data, 
            sector=add_item_form.sector.data,
            comments=add_item_form.comments.data, 
            watchlist_id=watchlist.id
        )
        db.session.add(item)
        db.session.commit()
        flash(f"The item '{item.ticker}' has been added to the watchlist.")
    elif add_item_form.errors:
        for error_name, error_desc in add_item_form.errors.items():
            error_name = error_name.title()
            flash(f"{error_name}: {error_desc[0]}")
    return redirect(url_for("watchlist.index"))


@bp.route('/<watch_name>/<int:id>/update', methods=['POST'])
@login_required
def update(id, watch_name):
    watchlists = get_watchlist_names()
    add_item_form = AddItemForm()
    add_item_form.watchlist.choices =  [
        (item, item)
        for item in watchlists
    ]
    if add_item_form.validate_on_submit():
        item = get_watchlist_item(id, watch_name)
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
            flash(f"The item '{item.ticker}' has been updated.")
    elif add_item_form.errors:
        for error_name, error_desc in add_item_form.errors.items():
            error_name = error_name.title()
            flash(f"{error_name}: {error_desc[0]}")
    return redirect(url_for("watchlist.index"))


@bp.route('/<watch_name>/<int:id>/delete', methods=['POST'])
@login_required
def delete(id, watch_name):
    item = get_watchlist_item(id, watch_name)
    if item:
        db.session.delete(item)
        db.session.commit()
        flash(f"The item '{item.ticker}' has been deleted from the watchlist.")
    return redirect(url_for('watchlist.index'))
