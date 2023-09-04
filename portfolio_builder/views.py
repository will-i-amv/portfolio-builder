from flask import Blueprint
from flask import current_app as app
from flask import render_template


bp = Blueprint("main", __name__)


@bp.route("/")
def index():
    app.logger.warning("sample message")
    return render_template("index.html")
