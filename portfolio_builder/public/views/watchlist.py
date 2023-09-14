from flask_login import current_user
from flask import Blueprint, flash, redirect, render_template, url_for
from flask_login import login_required
from sqlalchemy.orm import aliased

from portfolio_builder import db
from portfolio_builder.public.forms import (
    WatchlistAddForm, WatchlistSelectForm, WatchlistAddItemForm
)
from portfolio_builder.public.models import Watchlist, WatchlistItem


bp = Blueprint("watchlist", __name__, url_prefix="/watchlist")


@bp.route("/", methods=('GET', 'POST'))
@login_required
def index():
    watchlists = (
        db
        .session
        .query(Watchlist)
        .with_entities(Watchlist.name)
        .filter_by(user_id=current_user.id)
        .order_by(Watchlist.id)
        .all()
    )
    select_form = WatchlistSelectForm()
    select_form.watchlist.choices =  [
        (item[0], item[0])
        for item in watchlists
    ]
    add_item_form = WatchlistAddItemForm()
    add_item_form.watchlist.choices =  [
        (item[0], item[0])
        for item in watchlists
    ]
    if select_form.validate_on_submit():
        watchlist_name = select_form.watchlist.data
    else:
        watchlist_name = next(iter(watchlists), '')
    watch_itm = aliased(WatchlistItem)
    watch = aliased(Watchlist)
    watchlist_items = (
        db
        .session
        .query(watch_itm)
        .join(watch, onclause=(watch_itm.watchlist_id==watch.id))
        .filter(
            watch.user_id == current_user.id,
            watch.name == watchlist_name,
        )
        .with_entities(
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
    return render_template(
        "public/watchlist.html", 
        select_form=select_form,
        add_watchlist_form=WatchlistAddForm(),
        add_item_form=add_item_form,
        watchlist_items=watchlist_items,
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


@bp.route('/add_item', methods=['POST'])
@login_required
def add_watchlist_item():
    return redirect(url_for('watchlist.index'))
