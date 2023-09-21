from flask_login import current_user
from flask_wtf import FlaskForm
from wtforms import validators as v
from wtforms import (
    DateTimeField, DecimalField, HiddenField, IntegerField, 
    StringField, SubmitField, SelectField, TextAreaField
)

from portfolio_builder import db
from portfolio_builder.public.models import (
    get_default_date, Security, Watchlist, 
    WatchlistItem
)


class SelectWatchlistForm(FlaskForm):
    watchlist = SelectField("Select a Watchlist",  validators=[v.InputRequired()])
    submit = SubmitField("Get Overview")


class AddWatchlistForm(FlaskForm):
    name = StringField(
        "Watchlist Name",
        validators=[v.InputRequired(), v.Length(min=1, max=25)]
    )
    submit = SubmitField("Add")

    def validate_name(self, name):
        name_check = (
            Watchlist
            .query
            .filter_by(user_id=current_user.id, name=name.data)
            .first()
        )
        if name_check is not None:
            raise v.ValidationError(
                "There is already a watchlist with the same name"
            )


class AddItemForm(FlaskForm):
    order_id = HiddenField("")
    watchlist = SelectField("Watchlist",  validators=[v.InputRequired()])
    ticker = StringField(
        "Ticker",
        validators=[v.Length(min=2, max=20), v.InputRequired()]
    )
    quantity = IntegerField(
        "Quantity",
        validators=[
            v.InputRequired(),
            v.NumberRange(min=-10000000, max=10000000)
        ]
    )
    price = DecimalField(
        "Price",
        validators=[
            v.InputRequired(),
            v.NumberRange(min=0, max=100000)
        ]
    )
    trade_date = DateTimeField("Trade Date", default=get_default_date)
    sector = StringField(
        "Sector",
        validators=[v.InputRequired()]
    )
    comments = TextAreaField(
        "Comments",
        validators=[v.Optional(), v.Length(max=140)]
    )
    submit = SubmitField("Add to Watchlist")

    def validate_ticker(self, ticker):
        ticker_db = (
            db
            .session
            .query(Security)
            .filter(Security.ticker==ticker.data)
            .first()
        )
        if not ticker_db:
            raise v.ValidationError(
                f'The ticker "{ticker.data}" is unavailable.'
            )
