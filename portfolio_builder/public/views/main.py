from flask import Blueprint, redirect, url_for
from flask.wrappers import Response


bp = Blueprint("main", __name__)


@bp.route("/")
def index() -> Response:
    return redirect(url_for("dashboard.index"))
