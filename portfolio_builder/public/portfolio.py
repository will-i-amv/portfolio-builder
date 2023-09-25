import pandas as pd
from collections import deque, namedtuple


class PositionSummary:
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

    def get_summary(self):
        """
        Returns a named tuple of the ticker, net position and the average
        price of the opens lots
        """
        Summary = namedtuple(
            "Summary",
            ["ticker", "quantity", "average_price"]
        )
        ticker = self.ticker
        quantity = self.net_position
        average_price = self.average_cost
        return Summary(ticker, quantity, average_price)


class PositionAccounting(PositionSummary):
    """
    Inherits from the Position Summary and applies accounting methods
    to a Position
    """

    def __init__(self, close_prices, trade_history):
        super().__init__(trade_history)
        self.close_prices = close_prices  # Daily market prices

    def performance_table(self):
        """
        Combines the position breakdown with the daily prices to calculate
        daily unrealised P&L. The Daily unrealised P&L is the difference
        between the postion's weighted average cost and the market
        price.
        """
        df = pd.DataFrame(self.close_prices, columns=["date", "price"])
        df = df.set_index("date")
        df["quantity"] = float("nan")
        df["avg_cost"] = float("nan")
        start_date = str(self.breakdown[0][0])
        df2 = df.loc[start_date:]
        df2 = df2.copy()  # copied to prevent chained assignment
        for row in self.breakdown:
            df2.at[str(row[0]), "quantity"] = row[1]
            df2.at[str(row[0]), "avg_cost"] = row[2]
        df2["quantity"] = df2["quantity"].fillna(method="ffill")
        df2["price"] = df2["price"].fillna(method="ffill")
        df2["avg_cost"] = df2["avg_cost"].fillna(method="ffill")
        df2["price"] = pd.to_numeric(df2["price"])
        df2.loc[df2['quantity'] <= 0, 'Long/Short'] = -1
        df2.loc[df2['quantity'] > 0, 'Long/Short'] = 1
        df2["pct_change"] = 100 * (
            ((df2["price"] - df2["avg_cost"]) / df2["avg_cost"]) *
            df2["Long/Short"]
        )
        df2["pct_change"] = round(df2["pct_change"], 3)
        df2 = df2.reset_index()
        df2 = df2[["date", "quantity", "avg_cost", "price", "pct_change"]]
        df2 = list(df2.itertuples(index=False))
        return df2

    def daily_valuations(self):
        """
        Combines the position breakdown with the daily prices to calculate
        daily market value. The Daily market value is the positions quantity
        multiplied by the market price.
        """
        df = pd.DataFrame(self.close_prices, columns=["date", "price"])
        df = df.set_index("date")
        df["quantity"] = float("nan")
        df["market_val"] = float("nan")
        # the prices starting from the first date the security was held
        # start_date = str(self.breakdown[0][0])
        start_date = self.breakdown[0][0]
        df2 = df.loc[start_date:]
        df2 = df2.copy()  # copied to prevent chained assignment
        # update the quantity at each date
        for row in self.breakdown:
            df2.at[str(row[0]), "quantity"] = row[1]
        df2["price"] = df2["price"].fillna(method="ffill")
        df2["quantity"] = df2["quantity"].fillna(method="ffill")
        df2["price"] = pd.to_numeric(df2["price"])
        df2["market_val"] = round((df2["price"] * df2["quantity"]), 3)
        df2 = df2[["market_val"]]
        new_name = f"market_val_{self.ticker}"
        new_header = {"market_val": new_name}
        df2 = df2.rename(columns=new_header)
        return df2


class PortfolioSummary:
    """
    This is a collection of the Positions for the user accounts, priced as of
    the latest market prices
    """

    def __init__(self, df):
        self.portfolio_breakdown = df

    def net_valuations(self):
        """
        returns the portfolios daily market value
        """
        valuation = self.portfolio_breakdown.copy()
        valuation["portfolio_val"] = valuation.sum(axis=1)
        valuation = valuation[["portfolio_val"]]
        return valuation

    def convert_flows(self, flows):
        """
        Using the Holding Period Return (HPR) methodology. Purchases of
        securities are accounted as fund inflows and the sale of securities are
        accounted as increases in cash.

        By creating the cumulative sum of these values we can maintain an
        accurate calculation of the HPR which can be distorted as purchases and
        sells are added to the trades.
        """
        df_flows = pd.DataFrame(flows, columns=["date", "flows"])
        df_flows["cash"] = float("nan")
        df_flows["inflows"] = float("nan")
        df_flows["date"] = df_flows["date"].astype(str)
        df_flows["cash"] = df_flows.loc[df_flows['flows'] > 0, "flows"]
        df_flows["inflows"] = df_flows.loc[df_flows['flows'] <= 0, "flows"]
        df_flows["cash"] = df_flows["cash"].cumsum()
        df_flows["inflows"] = df_flows["inflows"].abs()
        df_flows = df_flows.set_index("date")  # need to sum groupby date
        df_flows = df_flows.groupby([df_flows.index]).sum()
        df_flows = df_flows.drop(columns=['flows'])
        df_flows = df_flows.replace({'cash': 0, 'inflows': 0}, float("nan"))
        return df_flows

    def generate_hpr(self, flows):
        """
        Where PortVal = Portfolio Value. The Formula for the Daily
        Holding Period Return (HPR) is calculated as follows:
        (Ending PortVal) / (Previous PortVal After Cash Flow) – 1.

        1. Add the cash from the sale of securities to the portfolio value.
        2. shift the total portfolio value column to allow us to easily
           caclulate the Percentage change before and after each cash flow.
        Returns a named tuple of daily HPR % changes.
        """
        df_flows = self.convert_flows(flows)
        valuation = self.net_valuations()
        valuation = valuation.join(df_flows)
        valuation["cash"] = valuation["cash"].fillna(method="ffill")
        valuation = valuation.fillna(value=0)
        valuation["total_portfolio_val"] = (
            valuation["portfolio_val"] +
            valuation["cash"]
        )
        valuation["portfolio_val"] = valuation["total_portfolio_val"].shift(1)
        valuation["pct_change"] = (
            (
                (valuation["total_portfolio_val"]) /
                (valuation["portfolio_val"] + valuation["inflows"])
            ) - 1
        ) * 100
        valuation["pct_change"] = round(valuation["pct_change"], 3)
        valuation = valuation.reset_index()
        valuation = list(valuation.itertuples(index=False))
        return valuation
