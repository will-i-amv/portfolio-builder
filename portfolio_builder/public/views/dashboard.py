import pandas as pd
from flask import Blueprint, request, render_template
from flask_login import login_required

from portfolio_builder.public.views.watchlist import get_watchlist_names
from portfolio_builder.public.portfolio import generate_hpr
from portfolio_builder.public.utils import (
    get_position_summary, get_portfolio_flows, get_portfolio_summary, 
)


bp = Blueprint('dashboard', __name__)


def get_pie_chart(portfolio_valuation):
    """
    Returns a named tuple of the largest positions by absolute exposure
    in descending order. For the portfolios that contain more than 6
    positions the next n positons are aggregated to and classified
    'as other'
    """
    df = portfolio_valuation.tail(1)
    df = df.T.reset_index()  # transpose table to make the tickers the rows
    if df.empty:
        return df
    new_headers = {df.columns[0]: "ticker", df.columns[1]: "Market_val"}
    df = df.rename(columns=new_headers)
    df["Market_val"] = abs(df["Market_val"])
    total_portfolio_val = sum(df["Market_val"])
    df["ticker"] = df["ticker"].replace("market_val_", "", regex=True)
    df["market_val_perc"] = round(df["Market_val"]/total_portfolio_val, 2)
    df = df[df["Market_val"] != 0]  # filter rows where valuation isnt zero
    df = df.sort_values(by=['market_val_perc'], ascending=False)
    if len(df) >= 7:
        # split the dataframe into two parts
        df_bottom = df.tail(len(df)-6)
        df = df.head(6)
        # sum the bottom dataframe to create an "Other" field
        df_bottom.loc['Other'] = df_bottom.sum(numeric_only=True, axis=0)
        df_bottom.at["Other", "ticker"] = "Other"
        df_bottom = df_bottom.tail(1)
        df_final = pd.concat([df, df_bottom])
        df_final = list(df_final.itertuples(index=False))
        return df_final
    else:
        df_final = list(df.itertuples(index=False))
        return df_final


def get_bar_chart(portfolio_valuation):
    """
    Returns a named tuple of the 5 largest positions by absolute exposure
    in descending order
    """
    df = portfolio_valuation.tail(1)
    df = df.T.reset_index()
    if df.empty:
        return df
    new_headers = {df.columns[0]: "ticker", df.columns[1]: "market_val"}
    df = df.rename(columns=new_headers)
    df["ticker"] = df["ticker"].replace("market_val_", "", regex=True)
    # sort the dataframe by largest exposures (descending order)
    df = df.iloc[df['market_val'].abs().argsort()]
    df = df[df["market_val"] != 0]  # filter rows where valuation isnt zero
    df = df.tail(5)  # the 5 largest positions by absolute mv
    df_final = list(df.itertuples(index=False))
    return df_final


@bp.route('/', methods=['GET', 'POST'])
@login_required
def index():
    watch_names = get_watchlist_names()
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
    flows = get_portfolio_flows(curr_watch_name)
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
