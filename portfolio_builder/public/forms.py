from flask_login import current_user
from flask_wtf import FlaskForm
from wtforms import validators as v
from wtforms import StringField, SubmitField, SelectField

from portfolio_builder.public.models import Watchlist


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
