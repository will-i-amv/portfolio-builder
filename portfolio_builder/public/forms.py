import datetime as dt
from typing import Optional

from flask_login import current_user
from flask_wtf import FlaskForm
from sqlalchemy.sql import func, case
from wtforms import (
    DateField, DecimalField, HiddenField, IntegerField,
    StringField, SubmitField, SelectField, TextAreaField
)
from wtforms.validators import (
    InputRequired, Length, NumberRange,
    Optional as OptionalField, ValidationError
)

from portfolio_builder import db
from portfolio_builder.public.models import (
    Security, Watchlist, WatchlistItem,
    WatchlistMgr, WatchlistItemMgr
)


def get_default_date(date_: Optional[dt.date] = None) -> dt.date:
    if date_ is None:
        date_ = dt.date.today()
    weekday = dt.date.isoweekday(date_)
    if weekday == 6:  # Saturday
        date_ = date_ - dt.timedelta(days=1)
    elif weekday == 7:  # Sunday
        date_ = date_ - dt.timedelta(days=2)
    return date_


class AddWatchlistForm(FlaskForm):
    name = StringField(
        "New Watchlist",
        validators=[
            InputRequired(),
            Length(min=3, max=25)
        ]
    )
    submit = SubmitField("Add")

    def validate_name(self, name: StringField) -> None:
        input_name = name.data
        watchlist = WatchlistMgr.get_first_item(filters=[
            Watchlist.user_id == current_user.id,  # type: ignore
            Watchlist.name == input_name,
        ])
        if watchlist is not None:
            raise ValidationError(
                f"The watchlist '{input_name}' already exists.")


class SelectWatchlistForm(FlaskForm):
    name = SelectField(
        "Available Watchlists",
        validators=[
            InputRequired(),
            Length(min=3, max=25)
        ]
    )
    submit = SubmitField("Select")

    def validate_name(self, name: SelectField) -> None:
        input_name = name.data
        watchlist = WatchlistMgr.get_first_item(filters=[
            Watchlist.user_id == current_user.id,  # type: ignore
            Watchlist.name == input_name,
        ])
        if watchlist is None:
            raise ValidationError(
                f"The watchlist '{input_name}' doesn't exist."
            )


def validate_date(form: FlaskForm, field: DateField) -> None:
    input_trade_date = field.data
    try:
        day_of_week = dt.date.isoweekday(input_trade_date)
    except TypeError:
        raise ValidationError("The trade date format is invalid.")
    curr_date = get_default_date()
    if day_of_week == 6 or day_of_week == 7:
        raise ValidationError(
            "The trade date can't fall on weekends."
        )
    elif input_trade_date > curr_date:
        raise ValidationError(
            "The trade date can't be a date in the future."
        )


class ItemForm(FlaskForm):
    watchlist = HiddenField("")
    ticker = StringField(
        "Ticker",
        validators=[
            InputRequired(),
            Length(min=1, max=20),
        ]
    )
    quantity = IntegerField(
        "Quantity",
        validators=[
            InputRequired(),
            NumberRange(min=1, max=100000)
        ]
    )
    price = DecimalField(
        "Price",
        validators=[
            InputRequired(),
            NumberRange(min=1, max=1000000)
        ]
    )
    side = SelectField(
        "Side",
        choices=[('buy'), ('sell')],
        validators=[InputRequired()]
    )
    trade_date = DateField(
        "Trade Date",
        validators=[InputRequired(), validate_date],
        default=get_default_date
    )
    comments = TextAreaField(
        "Comments",
        validators=[OptionalField(), Length(max=140)]
    )

    def validate_ticker(self, ticker: StringField) -> None:
        input_ticker = ticker.data
        tickers = (
            db
            .session
            .query(Security)
            .filter(Security.ticker == input_ticker)
            .first()
        )
        if not tickers:
            raise ValidationError(
                f"The ticker '{input_ticker}' doesn't exist in the database."
            )

    def validate_side(self):
        raise NotImplementedError

    def validate_trade_date(self):
        raise NotImplementedError


class AddItemForm(ItemForm):
    submit = SubmitField("Add Item")

    def validate_side(self, side: SelectField) -> None:
        input_side = side.data
        if input_side == 'sell':
            raise ValidationError(
                f"You can't sell if your portfolio is empty."
            )

    def validate_trade_date(self, trade_date: DateField) -> None:
        pass


class UpdateItemForm(ItemForm):
    submit = SubmitField("Update Item")

    def validate_side(self, side: SelectField) -> None:
        input_side = side.data
        input_ticker = self.ticker.data
        total_amount_sold = self.price.data * self.quantity.data
        if input_side == 'sell':
            net_asset_values = [
                item.flows
                for item in WatchlistItemMgr.get_items(
                    filters=[
                        Watchlist.user_id == current_user.id,  # type: ignore
                        Watchlist.name == self.watchlist.data,
                        WatchlistItem.ticker == self.ticker.data,
                    ],
                    entities=[
                        func.sum(
                            WatchlistItem.quantity * WatchlistItem.price * case(
                                (WatchlistItem.side == 'buy', 1),
                                (WatchlistItem.side == 'sell', (-1)),
                            )
                        )
                        .label('flows')
                    ]
                )
            ]
            net_asset_value = next(iter(net_asset_values), 0.0)
            if total_amount_sold > net_asset_value:
                raise ValidationError(
                    f"You tried to sell USD {total_amount_sold} " +
                    f"worth of '{input_ticker}', but you only have " +
                    f"USD {net_asset_value} in total."
                )

    def validate_trade_date(self, trade_date: DateField) -> None:
        input_trade_date = trade_date.data
        input_ticker = self.ticker.data
        watch_item_obj = (
            WatchlistItemMgr.get_first_item(filters=[
                Watchlist.user_id == current_user.id,  # type: ignore
                Watchlist.name == self.watchlist.data,
                WatchlistItem.ticker == self.ticker.data,
                WatchlistItem.is_last_trade == True,
            ])
        )
        if watch_item_obj:
            last_trade_date = watch_item_obj.trade_date
            if input_trade_date < last_trade_date:
                raise ValidationError(
                    f"The last trade date for ticker '{input_ticker}' " +
                    f"is '{last_trade_date}', the new date can't be before that."
                )
