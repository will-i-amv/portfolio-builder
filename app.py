from flask_migrate import Migrate

from portfolio_builder import create_app, db
from portfolio_builder.auth.models import User
from portfolio_builder.public.models import Security, Price, Watchlist, WatchlistItem


app = create_app()
migrate = Migrate(app, db)


@app.shell_context_processor
def make_shell_context():
    return {
        "db": db,
        "User": User,
        "Security": Security,
        "Price": Price,
        "Watchlist": Watchlist,
        "WatchlistItem": WatchlistItem
    }


@app.cli.command()
def init_db():
    from portfolio_builder.tasks import load_securities
    load_securities()
