from flask import Blueprint, render_template
from flask_login import login_required


bp = Blueprint('dashboard', __name__, url_prefix="/dashboard")


@bp.route('/', methods=['GET', 'POST'])
@login_required
def index():
    curr_watchlist_name = ''
    return render_template(
        'public/dashboard.html',
        summary=[], 
        line_chart=[],
        pie_chart=[], 
        bar_chart=[],
        watch_names=[curr_watchlist_name], 
        curr_watchlist_name=curr_watchlist_name,
    )
