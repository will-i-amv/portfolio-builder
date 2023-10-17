import datetime as dt

from flask_login import current_user
from flask_wtf import FlaskForm
from sqlalchemy.sql import func, case
from wtforms import validators as v
from wtforms import (
    DateField, DecimalField, HiddenField, IntegerField, 
    StringField, SubmitField, SelectField, TextAreaField
)

from portfolio_builder import db
from portfolio_builder.public.models import (
    get_watch_item, get_default_date, 
    Security, Watchlist, WatchlistItem
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

    def validate_name(self, name: StringField) -> None:
        name_check = (
            Watchlist
            .query
            .filter_by(user_id=current_user.id, name=name.data) # type: ignore
            .first()
        )
        if name_check is not None:
            raise v.ValidationError(
                "There is already a watchlist with the same name"
            )


class AddItemForm(FlaskForm):
    watch_name = HiddenField("")
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
    side = SelectField(
        "Transaction Type",
        choices=['buy', 'sell'],
        validators=[v.InputRequired()]
    )
    trade_date = DateField(
        "Trade Date", 
        validators=[v.InputRequired()],
        default=get_default_date
    )
    comments = TextAreaField(
        "Comments",
        validators=[v.Optional(), v.Length(max=140)]
    )
    submit = SubmitField("Add to Watchlist")

    def validate_ticker(self, ticker: StringField) -> None:
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

    def validate_trade_date(self, trade_date: DateField):
        """
        The trade date of a ticker can't be in the future, 
        on weekends, or before the last trade, if it exists.
        """
        input_trade_date = trade_date.data
        try:
            day_of_week = dt.date.isoweekday(input_trade_date)
        except TypeError:
            raise v.ValidationError("The trade date format is invalid.")
        curr_date = get_default_date()
        if day_of_week == 6 or day_of_week == 7:
            raise v.ValidationError(
                "The trade date cannot fall on weekends."
            )
        elif input_trade_date > curr_date:
            raise v.ValidationError(
                "The trade date cannot be a date in the future."
            )
        last_trade_date = (
            get_watch_item(filter=[
                Watchlist.name == self.watch_name.data,
                WatchlistItem.ticker == self.ticker.data,
            ])
            .with_entities(
                WatchlistItem.trade_date
            )
            .order_by(WatchlistItem.trade_date.desc())
            .first()
        )
        if last_trade_date:
            last_trade_date = next(iter(last_trade_date), '')
            if input_trade_date < last_trade_date:
                raise v.ValidationError(
                    f"The last trade date for ticker '{self.ticker.data}' " + 
                    "is {last_trade_date}, the new date can't be before that."
                )

    def validate_side(self, side: SelectField) -> None:
        input_side = side.data
        total_amount = self.price.data * self.quantity.data
        if input_side == 'sell':
            net_assets = (
                get_watch_item(filter=[
                    Watchlist.name == self.watch_name.data,
                    WatchlistItem.ticker == self.ticker.data,
                ])
                .with_entities(
                    func.sum(
                        WatchlistItem.quantity * WatchlistItem.price * case(
                            (WatchlistItem.side == 'buy', 1),
                            (WatchlistItem.side == 'sell', (-1)),
                        )
                    )
                    .label('flows')
                )
                .first()
            )
            net_assets = next(iter(net_assets), '')
            if not net_assets:
                raise v.ValidationError(f"You can't sell if your portfolio is empty.")
            elif total_amount > net_assets: 
                raise v.ValidationError(
                    f"You tried to sell USD {total_amount} worth of '{self.ticker.data}'," + 
                    f"but you only have USD {net_assets} in total." 
                )
