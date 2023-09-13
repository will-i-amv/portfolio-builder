from flask_wtf import FlaskForm
from wtforms import validators as v
from wtforms import SubmitField, SelectField


class WatchlistSelectForm(FlaskForm):
    watchlist = SelectField("Select a Watchlist",  validators=[v.InputRequired()])
    submit = SubmitField("Get Overview")
