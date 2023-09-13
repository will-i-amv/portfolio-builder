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
    watchlist_select_form = WatchlistSelectForm()
    watchlist_add_form = WatchlistAddForm()
    watchlists = []
    return render_template(
        "public/watchlist.html", 
        watchlist_select_form=watchlist_select_form,
        watchlist_add_form=watchlist_add_form,
        watchlists=watchlists
    )


@bp.route('/add', methods=['POST'])
@login_required
def add_watchlist():
    return redirect(url_for('watchlist.index'))
