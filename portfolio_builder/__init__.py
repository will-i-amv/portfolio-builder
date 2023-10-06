import datetime as dt
import logging.config
from typing import Dict, Any

from flask import Flask
from flask_apscheduler import APScheduler
from flask_bootstrap import Bootstrap
from flask_login import LoginManager
from flask_sqlalchemy import SQLAlchemy

from portfolio_builder.settings import Config


db = SQLAlchemy()
bootstrap = Bootstrap()
login_manager = LoginManager()
login_manager.login_view = 'auth.login' # type: ignore
scheduler = APScheduler()


def configure_logging() -> None:
    logging.config.dictConfig(
        {
            "version": 1,
            "formatters": {
                "default": {
                    "format": "[%(asctime)s] %(levelname)s in %(module)s: %(message)s",
                }
            },
            "handlers": {
                "wsgi": {
                    "class": "logging.StreamHandler",
                    "stream": "ext://sys.stdout",
                    "formatter": "default",
                }
            },
            "root": {"level": "INFO", "handlers": ["wsgi"]},
        }
    )


def create_app(config_overrides: Dict[str, Any] = {}) -> Flask:
    configure_logging()  # should be configured before any access to app.logger
    app = Flask(__name__)
    app.config.from_object(Config)
    app.config.from_prefixed_env()

    db.init_app(app)
    bootstrap.init_app(app)
    login_manager.init_app(app)
    scheduler.init_app(app)
    
    if config_overrides:
        app.config.from_mapping(config_overrides)

    from portfolio_builder.public.views.dashboard import bp as dashboard_bp
    from portfolio_builder.public.views.watchlist import bp as watchlist_bp
    from portfolio_builder.auth.views import bp as auth_bp
    app.register_blueprint(auth_bp)
    app.register_blueprint(dashboard_bp)
    app.register_blueprint(watchlist_bp)

    from portfolio_builder.tasks import load_prices_all_tickers
    scheduler.add_job(
        id='update_db_last_prices',
        func=load_prices_all_tickers, 
        trigger='interval',
        start_date=dt.datetime.combine(
            dt.date.today() + dt.timedelta(days=1), 
            dt.time(1, 0)
        ),
        days=1, 
    ) # task executes periodically, every day at 1am, starting tomorrow.
    scheduler.start()

    return app
