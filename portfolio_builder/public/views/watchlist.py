from flask import Blueprint, render_template
from flask_login import login_required

from portfolio_builder.public.forms import WatchlistSelectForm


bp = Blueprint("watchlist", __name__, url_prefix="/watchlist")


@bp.route("/", methods=('GET', 'POST'))
@login_required
def index():
    select_form = WatchlistSelectForm()
    watchlists = []
    return render_template(
        "public/watchlist.html", 
        form=select_form,
        watchlists=watchlists
    )
