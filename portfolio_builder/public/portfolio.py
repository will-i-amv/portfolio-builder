import pandas as pd
from collections import deque


class FifoAccounting:
    """
    Takes the trade history for a user's watchlist from the database and it's
    ticker. Then applies the FIFO accounting methodology to calculate the
    overall positions status i.e. final open lots, average cost and a breakdown
    of the open lots.

    This is a queue data structure.
    """

    def __init__(self, trade_history):
        self.trade_history = trade_history
        self.buy_quantities = deque([])
        self.buy_prices = deque([])
        self.buy_dates = deque([])
        self.sell_quantities = deque([])
        self.sell_prices = deque([])
        self.sell_dates = deque([])
        self.breakdown = []
        self.open_direction = ''
        self.net_position = 0
        self.average_cost = 0
        self.ticker = self.set_ticker()
        self.calc_fifo()

    def __repr__(self):
        return (
            f"<Ticker: {self.ticker}, " + 
            f"Quantity: {self.net_position}, " + 
            f"Average Price: {self.average_cost}>"
        )

    def set_ticker(self):
        # tickers = set([i.ticker for i in self.trade_history])
        tickers = set([trade.ticker for trade in self.trade_history])
        if len(tickers) == 1:
            return self.trade_history[0].ticker
        else:
            raise ValueError(
                "The Trade History for this security " +
                "contains multiple tickers"
            )

    def calc_total_open_lots(self):
        """ returns the sum of the positions open lots"""
        if self.open_direction == "long":
            return sum(self.buy_quantities)
        elif self.open_direction == "short":
            return sum(self.sell_quantities)
        else:
            return 0

    def calc_total_market_value(self):
        """Returns the position's market value"""
        total = 0
        if self.buy_quantities and self.open_direction == "long":
            zipped = zip(self.buy_quantities, self.buy_prices)
            total = (quantity*price for quantity, price in zipped)
        elif self.sell_quantities and self.open_direction == "short":
            zipped = zip(self.sell_quantities, self.sell_prices)
            total = (quantity*price for quantity, price in zipped)
        return sum(total) if total != 0 else 0

    def calc_average_cost(self):
        """Returns the weighted average cost of the positions open lots."""
        open_lots = self.calc_total_open_lots()
        market_value = self.calc_total_market_value() 
        if open_lots == 0 or not open_lots:
            return 0
        average_cost = abs(market_value / open_lots)
        return round(average_cost, 4)

    def set_direction(self):
        """
        Checks if there has been a reversal in the users overall
        trade direction and sets that direction accordingly.
        """
        if self.open_direction == "short" and self.net_position > 0:
            self.open_direction = "long"
        elif self.open_direction == "long" and self.net_position < 0:
            self.open_direction = "short"

    def add_trade(self, side, units, price, date):
        if side == "buy":
            self.buy_quantities.append(units)
            self.buy_prices.append(price)
            self.buy_dates.append(date)
        elif side == "sell":
            self.sell_quantities.append(units)
            self.sell_prices.append(price)
            self.sell_dates.append(date)

    def remove_trade(self, direction):
        if direction == "buy":
            popped_quantity = self.buy_quantities.popleft()
            self.buy_prices.popleft()
            self.buy_dates.popleft()
        elif direction == "sell":
            popped_quantity = self.sell_quantities.popleft()
            self.sell_prices.popleft()
            self.sell_dates.popleft()
        return popped_quantity

    def set_initial_trade(self):
        units = self.trade_history[0].quantity
        price = self.trade_history[0].price
        # date = self.trade_history[0].trade_date
        date = self.trade_history[0].date
        if units >= 0:
            self.open_direction = "long"
            self.add_trade("buy", units, price, date)

        else:
            self.open_direction = "short"
            self.add_trade("sell", units, price, date)
        self.average_cost = self.calc_average_cost()
        self.net_position = self.calc_total_open_lots()
        self.breakdown.append([date, self.net_position, self.average_cost])

    def collapse_trade(self):
        if self.sell_quantities:
            if self.sell_quantities[0] >= 0:
                self.remove_trade("sell")
        if self.buy_quantities:
            if self.buy_quantities[0] <= 0:
                self.remove_trade("buy")

    def calc_fifo(self):
        """
        This algorithm iterate over the trade history. It sets the
        initial trade direction to get the initial open lots and then increases
        or closes lots based on each trade.

        In the event that a position was initally long then becomes short or
        vice versa the open lots will be increased or closed accordingly.
        """
        if self.trade_history:
            self.set_initial_trade()
        else:
            return []

        counter = 1
        while counter < len(self.trade_history):
            units = self.trade_history[counter].quantity
            price = self.trade_history[counter].price
            # date = self.trade_history[counter].trade_date
            date = self.trade_history[counter].date
            
            # Both trades have the same sign
            if units*self.net_position > 0:
                if self.open_direction == "long":
                    self.add_trade("buy", units, price, date)
                else:
                    self.add_trade("sell", units, price, date)
            
            # The position is flat
            elif units*self.net_position == 0:
                if units >= 0:
                    self.open_direction = "long"
                    self.add_trade("buy", units, price, date)
                else:
                    self.open_direction = "short"
                    self.add_trade("sell", units, price, date)
            
            # Both trades are in different directions
            else:
                if self.open_direction == "long":
                    self.add_trade("sell", units, price, date)
                    while self.sell_quantities and self.buy_quantities:
                        if abs(self.sell_quantities[0]) >= self.buy_quantities[0]:
                            self.sell_quantities[0] += self.buy_quantities[0]
                            self.remove_trade("buy")
                        else:
                            self.buy_quantities[0] += self.remove_trade("sell")
                    self.net_position += units 
                else:
                    self.add_trade("buy", units, price, date)
                    while self.sell_quantities and self.buy_quantities:
                        if self.buy_quantities[0] >= abs(self.sell_quantities[0]):
                            self.buy_quantities[0] += self.sell_quantities[0]
                            self.remove_trade("sell")
                        else:
                            self.sell_quantities[0] += self.remove_trade("buy")
                    self.net_position += units

            self.collapse_trade()
            self.set_direction()
            self.average_cost = self.calc_average_cost()
            self.net_position = self.calc_total_open_lots()
            self.breakdown.append([date, self.net_position, self.average_cost])
            counter += 1


def calc_daily_valuations(ticker, prices, summaries):
    """
    Combines the position breakdown with the daily prices to calculate
    daily market value. The Daily market value is the positions quantity
    multiplied by the market price.
    """
    df_summaries = (
        summaries
        .astype({
            # 'date': 'datetime64[ns]', 
            'date': 'str', 
            'quantity': 'float64', 
            'average_price': 'float64',
        })
    )
    df_prices = (
        pd
        .DataFrame(
            data=prices, 
            columns=["date", "price"]
        )
        .astype({
            # 'date': 'datetime64[ns]', 
            'date': 'str', 
            'price': 'float64'
        })
    )
    df_cleaned = (
        pd
        .merge(df_prices, df_summaries, on=['date'], how='left')
        .astype({
            'quantity': 'float64', 
            'price': 'float64',
        })
        .fillna(method='ffill')
        .assign(market_val=lambda x: x['quantity'] * x['price'])
        .round({'market_val': 3})
        .loc[:, ['date', 'market_val']]
        .rename(columns={'market_val': f'market_val_{ticker}'})
        .set_index('date')
        .dropna()
    )
    return df_cleaned


def convert_flows(flows):
    """
    Using the Holding Period Return (HPR) methodology. Purchases of
    securities are accounted as fund inflows and the sale of securities are
    accounted as increases in cash.

    By creating the cumulative sum of these values we can maintain an
    accurate calculation of the HPR which can be distorted as purchases and
    sells are added to the trades.
    """
    df_flows = (
        pd
        .DataFrame(flows, columns=["date", "flows"])
        .astype({'date': 'str'})
        .assign(
            cash=lambda x: x.loc[x['flows'] > 0, 'flows'].cumsum(),
            inflows=lambda x: x.loc[x['flows'] <= 0, 'flows'].abs(), # This should also be .cumsum()
        )
        .set_index("date")
        .assign(cash=lambda x: x['cash'].ffill())
        .fillna(0)
        .drop(columns=['flows'])
    )
    return df_flows


def generate_hpr(df_summary, flows):
    """
    Where PortVal = Portfolio Value. The Formula for the Daily
    Holding Period Return (HPR) is calculated as follows:
    (Ending PortVal) / (Previous PortVal After Cash Flow) – 1.

    1. Add the cash from the sale of securities to the portfolio value.
    2. shift the total portfolio value column to allow us to easily
        caclulate the Percentage change before and after each cash flow.
    Returns a named tuple of daily HPR % changes.
    """
    df_flows = convert_flows(flows)
    df_valuation =( 
        df_summary
        .assign(portfolio_val=lambda x: x.sum(axis=1))
        .loc[:, ['portfolio_val']]
    )
    df = (
        pd
        .merge(df_valuation, df_flows, left_index=True, right_index=True, how='left')
        .assign(cash=lambda x: x['cash'].ffill())
        .fillna(0)
        .assign(
            total_portfolio_val=lambda x: x["portfolio_val"] + x["cash"]
        )
        .assign(
            total_portfolio_val_prev=lambda x: x['total_portfolio_val'].shift(1)
        )
        .assign(
            pct_change=lambda x: (
                (
                    x["total_portfolio_val"] / 
                    (x["total_portfolio_val_prev"] + x["inflows"])
                ) - 1
            ) * 100
        )
        .round({'pct_change': 3})
        .fillna({'pct_change': 0.0})
        .loc[:, ['pct_change']]
        .reset_index()
    )
    return list(df.itertuples(index=False))
