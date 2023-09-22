from flask import Blueprint
from flask import redirect, url_for


bp = Blueprint("main", __name__)


@bp.route("/")
def index():
    return redirect(url_for("dashboard.index"))
