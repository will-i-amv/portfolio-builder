from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField, PasswordField, BooleanField, ValidationError
from wtforms.validators import DataRequired, Length, EqualTo, Regexp
from portfolio_builder.auth.models import User


class LoginForm(FlaskForm):
    username = StringField("Username", validators=[DataRequired()])
    password = PasswordField("Password", validators=[DataRequired()])
    remember_me = BooleanField("Keep me logged in")
    submit = SubmitField("Sign In")


class RegistrationForm(FlaskForm):
    username = StringField(
        "Username",
        validators=[
            Length(min=4, max=25),
            Regexp(
                '^[A-Za-z][A-Za-z0-9_.]*$',
                0,
                'Usernames must have only letters, numbers, dots or underscores'
            )
        ]
    )
    password = PasswordField(
        "New Password",
        validators=[
            DataRequired(),
            Length(min=8, max=32),
            EqualTo("password2", message="Passwords must match")
        ]
    )
    password2 = PasswordField('Confirm password', validators=[DataRequired()])
    submit = SubmitField("Register")

    def validate_username(self, field):
        if User.query.filter_by(username=field.data).first():
            raise ValidationError('Username already in use.')
