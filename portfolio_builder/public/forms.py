import datetime as dt

from flask_login import current_user
from flask_wtf import FlaskForm
from wtforms import validators as v
from wtforms import (
    DateTimeField, DecimalField, HiddenField, IntegerField, 
    StringField, SubmitField, SelectField, TextAreaField
)

from portfolio_builder.public.models import (
    get_default_date, Security, Watchlist, 
    WatchlistItem
)


class WatchlistSelectForm(FlaskForm):
    watchlist = SelectField("Select a Watchlist",  validators=[v.InputRequired()])
    submit = SubmitField("Get Overview")


class WatchlistAddForm(FlaskForm):
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


class WatchlistAddItemForm(FlaskForm):
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
        ticker_check = (
            Security
            .query
            .filter_by(ticker=ticker.data)
            .first()
        )
        if ticker_check is None:
            raise v.ValidationError(
                f'The ticker {ticker.data} is unavailable.'
            )

    def validate_trade_date(self, trade_date):
        # Check if the trade exists.
        timestamp = (
            WatchlistItem
            .query
            .with_entities(WatchlistItem.created_timestamp)
            .filter_by(id=self.order_id.data)
            .first()
        )
        if timestamp is None:
            return True
        trade_date = trade_date.data
        max_date = get_default_date()
        # Users can amend dates up to 100 days prior to the created_timestamp.
        lowest_date = timestamp[0] - dt.timedelta(days=100)
        try:
            day_of_week = dt.date.isoweekday(trade_date)
        except TypeError:
            raise v.ValidationError("Not a valid datetime value.")
        if trade_date > max_date:
            raise v.ValidationError(
                "The trade and time cannot be a date in the future"
            )
        elif trade_date < lowest_date:
            raise v.ValidationError(
                "The trade and time must be within " +
                "100 days of the order creation date"
            )
        elif day_of_week == 6 or day_of_week == 7:
            raise v.ValidationError(
                "The trade date cannot fall on weekends."
            )

