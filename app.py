import datetime as dt
from typing import Dict, Any

from flask_migrate import Migrate

from portfolio_builder import create_app, db
from portfolio_builder.auth.models import User
from portfolio_builder.public.models import Security, Price, Watchlist, WatchlistItem
from portfolio_builder.public.tasks import load_securities, load_prices


app = create_app(settings_name='production')
migrate = Migrate(app, db)


@app.shell_context_processor
def make_shell_context() -> Dict[str, Any]:
    return {
        "db": db,
        "User": User,
        "Security": Security,
        "Price": Price,
        "Watchlist": Watchlist,
        "WatchlistItem": WatchlistItem
    }


@app.cli.command()
def init_db() -> None:
    common_tech_stocks = [
        'META', 
        'GOOGL', 
        'AAPL', 
        'AMZN', 
        'MSFT', 
        'NFLX',
    ]
    end_date = dt.date.today() - dt.timedelta(days=1)
    start_date = end_date - dt.timedelta(days=100)
    load_securities()
    load_prices(common_tech_stocks, start_date, end_date)
