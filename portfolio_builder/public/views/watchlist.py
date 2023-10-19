import datetime as dt

from flask import Blueprint, request, flash, redirect, render_template, url_for
from werkzeug.wrappers.response import Response
from flask_login import current_user, login_required
from flask_wtf import FlaskForm

from portfolio_builder import db, scheduler
from portfolio_builder.public.forms import (
    AddWatchlistForm, SelectWatchlistForm, AddItemForm
)
from portfolio_builder.public.models import (
    Watchlist, WatchlistItem,
    get_securities, get_first_watchlist, get_watchlists, get_first_watch_item, get_watch_items
)
from portfolio_builder.public.tasks import load_prices_ticker


bp = Blueprint("watchlist", __name__, url_prefix="/watchlist")


def flash_errors(form: FlaskForm, category="warning"):
    """Flash all errors for a form."""
    for field, errors in form.errors.items():
        for error in errors:
            flash(
                f"{getattr(form, field).label.text} - {error}",  # type: ignore
                category
            )
            

@bp.route("/", methods=['GET', 'POST'])
@login_required
def index() -> str:
    """
    Renders the watchlist page and handles watchlist-related actions.

    Returns:
        str: A rendered HTML template with the necessary data.
    """
    watch_names = [
        item.name
        for item in get_watchlists(
            filters=[Watchlist.user_id==current_user.id], # type: ignore 
        )
    ]
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
    watch_items = get_watch_items(filters=[
        Watchlist.user_id==current_user.id, # type: ignore
        Watchlist.name == curr_watch_name,
        WatchlistItem.is_last_trade == True,
    ])
    securities = get_securities()
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
    """
    Adds a new watchlist to the database.

    Returns:
        Response: A redirect response to the 'watchlist.index' endpoint.
    """
    watchlist_form = AddWatchlistForm()
    if watchlist_form.errors:
        flash_errors(watchlist_form)
    elif watchlist_form.validate_on_submit():
        watchlist_name = watchlist_form.name.data 
        new_watchlist = Watchlist(
            user_id=current_user.id, # type: ignore
            name=watchlist_name, 
        )
        db.session.add(new_watchlist)
        db.session.commit()
        flash(f"The watchlist '{watchlist_name}' has been added.")
    return redirect(url_for('watchlist.index'))


@bp.route('/delete_watchlist', methods=['POST'])
@login_required
def delete_watchlist() -> Response:
    """
    Deletes a watchlist from the database.

    :return: A redirect response to the 'watchlist.index' route.
    """
    if not request.method == "POST":
        return redirect(url_for('watchlist.index'))
    else:
        watch_name = request.form.get('watchlist_group_removed')
        watchlist = get_first_watchlist(filters=[Watchlist.name==watch_name])
        if not watchlist:
            flash(f"The watchlist '{watch_name}' does not exist.")
        else:
            db.session.delete(watchlist)
            db.session.commit()
            flash(f"The watchlist '{watch_name}' has been deleted.")
        return redirect(url_for('watchlist.index'))


@bp.route('/<watch_name>/add', methods=['POST'])
@login_required
def add(watch_name: str) -> Response:
    """
    Adds a new item to a specific watchlist in the database.

    Args:
        watch_name (str): The name of the watchlist to add the item to.

    Returns:
        Response: A redirect response to the `watchlist.index` endpoint.
    """
    add_item_form = AddItemForm()
    if add_item_form.errors:
        flash_errors(add_item_form)
    elif add_item_form.validate_on_submit():
        watchlist = get_first_watchlist(filters=[Watchlist.name==watch_name])
        if not watchlist:
            flash(f"The watchlist '{watch_name}' does not exist.")
        else:
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
    return redirect(url_for("watchlist.index"))


@bp.route('/<watch_name>/<ticker>/update', methods=['POST'])
@login_required
def update(watch_name: str, ticker: str) -> Response:
    """
    Updates a watchlist item, identified by the ticker, 
    of a specific watchlist in the database.

    Args:
        watch_name (str): The name of the watchlist to update.
        ticker (str): The ticker symbol of the watchlist item to update.

    Returns:
        Response: Redirects the user to the watchlist index page.
    """
    add_item_form = AddItemForm()
    if add_item_form.errors:
        flash_errors(add_item_form)
    elif add_item_form.validate_on_submit():
        last_item = get_first_watch_item(filters=[
            Watchlist.user_id==current_user.id, # type: ignore
            Watchlist.name == watch_name,
            WatchlistItem.ticker == ticker,
            WatchlistItem.is_last_trade == True,
        ])
        if not last_item:
            flash(f"There are no items of ticker '{ticker}' to update.")
        else:
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
    return redirect(url_for("watchlist.index"))


@bp.route('/<watch_name>/<ticker>/delete', methods=['POST'])
@login_required
def delete(watch_name: str, ticker: str) -> Response:
    """
    Deletes a specific ticker from a watchlist.

    Args:
        watch_name (str): The name of the watchlist from which the ticker should be deleted.
        ticker (str): The ticker symbol of the stock to be deleted from the watchlist.

    Returns:
        Response: Redirects the user to the watchlist index page.
    """
    ids = [
        item.id
        for item in get_watch_items(
            filters=[
                Watchlist.user_id==current_user.id, # type: ignore
                Watchlist.name == watch_name,
                WatchlistItem.ticker == ticker,
            ],
            entities=[WatchlistItem.id]
        )
    ]
    if not ids:
        flash(
            f"An error occurred while trying to delete " + 
            f"the items of ticker '{ticker}' from watchlist '{watch_name}'."
        )
    else:
        db.session.query(WatchlistItem).filter(WatchlistItem.id.in_(ids)).delete()
        db.session.commit()
        flash(
            f"The items of ticker '{ticker}' have been deleted " + 
            f"from watchlist '{watch_name}'."
        )
    return redirect(url_for('watchlist.index'))
