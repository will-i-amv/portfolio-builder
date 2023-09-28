import pandas as pd
from flask import Blueprint, request, render_template
from flask_login import login_required

from portfolio_builder.public.models import (
    Watchlist, get_all_watch_names, get_watch_flows
)
from portfolio_builder.public.portfolio import generate_hpr
from portfolio_builder.public.utils import (
    get_position_summary, get_portfolio_summary, 
)


bp = Blueprint('dashboard', __name__)


def get_pie_chart(df_portfolio):
    """
    Returns a named tuple of the largest positions by absolute exposure
    in descending order. For the portfolios that contain more than 6
    positions the next n positons are aggregated to and classified
    as 'Other'
    """
    if df_portfolio.empty:
        return list(df_portfolio.itertuples(index=False))
    df_initial = (
        df_portfolio
        .tail(1)
        .T
        .reset_index()
    )
    df_initial.columns = ['ticker', 'Market_val']
    df_temp = (
        df_initial
        .assign(Market_val=lambda x: x["Market_val"].abs())
        .replace({'ticker': {'market_val_': ''}}, regex=True)
        .assign(
            market_val_perc=lambda x: 
                (x["Market_val"]  / x["Market_val"].sum()) * 100
        )
        .round({'market_val_perc': 2})
        .loc[lambda x: x["Market_val"] != 0.0]
        .sort_values(by=['market_val_perc'], ascending=False)
    )
    max_len = 6
    df_len = df_temp.shape[0] 
    if df_len < max_len:
        return list(df_temp.itertuples(index=False))
    else:
        df_top = df_temp.head(max_len)
        df_bottom = (
            df_temp
            .tail(df_len - max_len)
            .pivot_table(
                index='ticker',
                margins=True,
                margins_name='Other', # defaults to 'All'
                aggfunc='sum'
            )
            .reset_index()
            .tail(1)
        )
        df_final = pd.concat([df_top, df_bottom])
        return list(df_final.itertuples(index=False))


def get_bar_chart(df_portfolio):
    """
    Returns a named tuple of the 5 largest positions by absolute exposure
    in descending order
    """
    if df_portfolio.empty:
        return list(df_portfolio.itertuples(index=False))
    df_initial = (
        df_portfolio
        .tail(1)
        .T
        .reset_index()
    )
    df_initial.columns = ['ticker', 'market_val']
    df_final = (
        df_initial
        .replace({'ticker': {'market_val_': ''}}, regex=True)
        .sort_values(by=['market_val'], ascending=False)
        .loc[lambda x: x["market_val"] != 0.0]
        .tail(5)
    )
    return list(df_final.itertuples(index=False))


@bp.route('/', methods=['GET', 'POST'])
@login_required
def index():
    watch_names = get_all_watch_names()
    if request.method == 'POST':
        curr_watch_name = request.form.get('watchlist_group_selection')
    else:
        curr_watch_name = next(iter(watch_names), '')
    summaries = get_position_summary(curr_watch_name)
    summary = [
        (
            df
            .assign(ticker=ticker)
            .sort_values(by=['date'])
            .drop(['date'], axis=1)
            .tail(1)
            .to_dict('records')[0]
        ) 
        for ticker, df in summaries.items()
    ]
    if len(summary) > 7:
        summary = summary[0:7]
    flows = get_watch_flows(filter=[Watchlist.name == curr_watch_name])
    portfolio = get_portfolio_summary(summaries)
    portfolio_hpr = generate_hpr(portfolio, flows)
    content = {
        'summary': summary, 
        'line_chart': portfolio_hpr,
        'pie_chart': get_pie_chart(portfolio), 
        'bar_chart': get_bar_chart(portfolio),
        'watch_names': watch_names, 
        'curr_watch_name': curr_watch_name
    }
    return render_template('public/dashboard.html', **content)
