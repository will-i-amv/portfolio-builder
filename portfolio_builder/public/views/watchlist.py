from flask_login import current_user
from flask import Blueprint, flash, redirect, render_template, url_for
from flask_login import login_required

from portfolio_builder import db
from portfolio_builder.public.forms import WatchlistAddForm, WatchlistSelectForm
from portfolio_builder.public.models import Watchlist


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
    watchlist_select_form = WatchlistSelectForm()
    watchlist_select_form.watchlist.choices =  [
        (item[0], item[0])
        for item in watchlists
    ]
    return render_template(
        "public/watchlist.html", 
        watchlist_select_form=watchlist_select_form,
        watchlist_add_form=WatchlistAddForm(),
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
