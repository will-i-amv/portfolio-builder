import logging.config
from typing import Dict, Any

from flask import Flask
from flask_bootstrap import Bootstrap
from flask_login import LoginManager
from flask_sqlalchemy import SQLAlchemy

from portfolio_builder.settings import Config


db = SQLAlchemy()
bootstrap = Bootstrap()
login_manager = LoginManager()
login_manager.login_view = 'auth.login'


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


def create_app(config_overrides: Dict[str, Any] = None) -> Flask:
    configure_logging()  # should be configured before any access to app.logger
    app = Flask(__name__)
    app.config.from_object(Config)
    app.config.from_prefixed_env()

    db.init_app(app)
    bootstrap.init_app(app)
    login_manager.init_app(app)
    
    if config_overrides is not None:
        app.config.from_mapping(config_overrides)

    from portfolio_builder.public.views.dashboard import bp as dashboard_bp
    from portfolio_builder.public.views.main import bp as main_bp
    from portfolio_builder.public.views.watchlist import bp as watchlist_bp
    from portfolio_builder.auth.views import bp as auth_bp
    app.register_blueprint(auth_bp)
    app.register_blueprint(dashboard_bp)
    app.register_blueprint(main_bp)
    app.register_blueprint(watchlist_bp)

    return app
