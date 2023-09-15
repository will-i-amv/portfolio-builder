from flask import Blueprint, flash, redirect, render_template, url_for
from flask_login import login_user, logout_user
from werkzeug.security import check_password_hash, generate_password_hash

from portfolio_builder import db
from portfolio_builder.auth.forms import RegistrationForm, LoginForm
from portfolio_builder.auth.models import User


bp = Blueprint("auth", __name__, url_prefix="/auth")


@bp.route("/register", methods=("GET", "POST"))
def register():
    form = RegistrationForm()
    if not form.validate_on_submit():
        return render_template("auth/register.html", title="Register", form=form)
    else:
        new_user = User(
            username=form.username.data,
            password=generate_password_hash(form.password.data)
        )
        db.session.add(new_user)
        db.session.commit()
        flash("Your account has been created")
        return redirect(url_for("auth.login"))


@bp.route("/login", methods=("GET", "POST"))
def login():
    form = LoginForm()
    if not form.validate_on_submit():
        return render_template("auth/login.html", title="Log In", form=form)
    else:
        user = User.query.filter_by(username=form.username.data).first()
        if not (
            (user is not None) and
            check_password_hash(user.password, form.password.data)
        ):
            flash("Invalid username or password")
            return redirect(url_for("auth.login"))
        else:
            login_user(user, remember=form.remember_me.data)
            return redirect(url_for("watchlist.index"))


@bp.route('/logout')
def logout():
    logout_user()
    flash('You have been logged out.')
    return redirect(url_for('main.index'))
