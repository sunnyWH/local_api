import clients
import logging
from algo_interface import Algo
import time

# USER IMPORTS
from datetime import time as datetimeTime
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from collections import deque
import pandas as pd
import numpy as np


class Monkey(Algo):
    def __init__(self):
        # CONSTANT PARAMETERS
        self.account = "FW077"
        self.product = "NQU5"
        self.productDiv = 100
        self.voteDiv = 2
        self.voteCount = 250
        self.finalVotesCount = 3
        self.check1 = 15
        self.check2 = 25
        self.check3 = 30
        self.check4 = 35
        self.gainLimit = 0.015
        self.lossLimit = 0.0035
        self.marketStart = datetimeTime(8, 30)
        self.preMarketStart = datetimeTime(8, 29)
        self.marketEnd = datetimeTime(14, 59)
        self.toPrint = True
        self.running = True
        self.ticksAway = 1_000

        # DYNAMIC PARAMETERS
        self.signal = 0
        self.signalFlip = 0
        self.votes = deque(maxlen=self.voteCount)
        self.finalVotes = deque(maxlen=self.finalVotesCount)
        self.votesFull = False
        self.printed = False
        self.printedClosed = False
        self.voteTotal = 0
        self.latestTradePrice = None
        self.lastPrice = None
        self.bid = None
        self.ask = None
        self.lastTime = None
        self.currentTime = None
        self.inMarket = False
        self.position = 0
        self.entryPrices = []
        self.initialTradeTime = None
        self.gain = None
        self.gainOrder = None
        self.loss = None
        self.lossOrder = None
        self.pnlCheckCounter = 0

    def warmup(self):
        engine = create_engine(
            "postgresql+psycopg2://tickreader:tickreader@tsdb:5432/cme",
            poolclass=NullPool,  # don't reuse connections — safe for scripts
            connect_args={"connect_timeout": 5},
        )
        query = f"""
            SELECT wh_name, t_price, sending_time
            FROM nq_fut_trades_weekly
            WHERE wh_name = %s
            ORDER BY sending_time DESC
            LIMIT 250_000
        """
        try:
            with engine.connect() as conn:
                df = pd.read_sql(query, conn, params=(self.product,))
        except Exception as e:
            print("Error:", e)
        finally:
            engine.dispose()
        df.drop(columns=["wh_name"], inplace=True)
        df["sending_time"] = df["sending_time"].str.replace(
            r"(\.\d{6})\d+", r"\1", regex=True
        )
        df["sending_time"] = pd.to_datetime(
            df["sending_time"], format="%Y-%m-%d %H:%M:%S.%f %z"
        )
        df.index = df["sending_time"]
        df = df.sort_index()
        times = df.index.time
        mask = (times >= pd.to_datetime("08:30").time()) & (
            times <= pd.to_datetime("15:00").time()
        )
        df = df[mask].copy()
        df["minute"] = df["sending_time"].dt.floor("min")
        df = df.loc[df.groupby("minute")["sending_time"].idxmin()]  # last traded price
        df = df.drop(columns=["sending_time", "minute"])
        df["t_price"] = df["t_price"] / self.productDiv
        moves = df["t_price"].diff()
        time_diff = moves.index.to_series().diff()
        moves[time_diff > pd.Timedelta(minutes=2)] = 0
        moves.dropna(inplace=True)
        for move in moves[:-3]:
            self.add_votes(move, toPrint=False)
            self.check_votes_for_final_votes(toPrint=False)
            self.check_signal_from_final_votes(toPrint=False)
        for move in moves[-3:]:
            self.add_votes(move, toPrint=self.toPrint)
            self.check_votes_for_final_votes(toPrint=self.toPrint)
            self.check_signal_from_final_votes(toPrint=self.toPrint)
        logging.info(f"MONKEY, WARMUP DONE")

    def run(self):
        self.warmup()
        while self.running:
            # GET BID, ASK, TRADE PRICE OUTSIDE MARKET HOURS
            self.currentTime = datetime.now()
            self.bid = clients.tradingClient.get_bid(self.product)
            self.ask = clients.tradingClient.get_ask(self.product)
            self.latestTradePrice = clients.tradingClient.get_trade_price(self.product)

            # do divisor adjustment for prices
            if any(x is None for x in [self.bid, self.ask, self.latestTradePrice]):
                continue
            self.bid = self.bid / self.productDiv
            self.ask = self.ask / self.productDiv
            self.latestTradePrice = self.latestTradePrice / self.productDiv

            if not self.inMarket:
                if (
                    self.lastTime is None
                    and self.preMarketStart <= self.currentTime.time()
                ):
                    self.lastTime = self.currentTime.replace(second=0, microsecond=0)
                if (
                    self.marketStart <= self.currentTime.time() <= self.marketEnd
                    and not self.printed
                ):
                    logging.info(f"In Market Hours")
                    self.printed = True
                    self.printedClosed = False
                    self.inMarket = True

            while self.inMarket and self.running:
                self.currentTime = datetime.now()
                self.bid = clients.tradingClient.get_bid(self.product)
                self.ask = clients.tradingClient.get_ask(self.product)
                self.latestTradePrice = clients.tradingClient.get_trade_price(
                    self.product
                )

                # do divisor adjustment for prices
                if any(x is None for x in [self.bid, self.ask, self.latestTradePrice]):
                    continue
                self.bid = self.bid / self.productDiv
                self.ask = self.ask / self.productDiv
                self.latestTradePrice = self.latestTradePrice / self.productDiv

                if self.lastTime is None:
                    self.lastTime = self.currentTime.replace(second=0, microsecond=0)

                if (
                    not self.marketStart <= self.currentTime.time() <= self.marketEnd
                    and not self.printedClosed
                ):
                    logging.info(f"MONKEY: Outside of Market Hours, flattening...")
                    self.flatten(self.latestTradePrice, "MARKET_FLATTEN")
                    self.signalFlip = 0
                    self.printed = False
                    self.printedClosed = True
                    self.inMarket = False
                    self.disconnect()

                else:
                    # check gains and stops at every update on bid/ask
                    orders_snapshot = list(clients.tradingClient.activeOrders.values())
                    for order in orders_snapshot:
                        if order.account == self.account:
                            if order.prefix == "G":
                                self.gainOrder = order
                            elif order.prefix == "D":
                                self.lossOrder = order

                    if self.position > 0:
                        if self.bid >= self.gain:
                            logging.info(f"GAIN HIT: {-self.position}, {self.gain}")
                            self.flatten(self.gain, "GAIN_FLATTEN")
                            self.signalFlip = 0
                        elif self.ask < self.loss:
                            logging.info(f"LOSS HIT: {-self.position}, {self.loss}")
                            if abs(self.position) != 1:
                                self.signalFlip = 0
                            self.flatten(self.loss, "LOSS_FLATTEN")
                    if self.position < 0:
                        if self.ask <= self.gain:
                            logging.info(f"GAIN HIT: {-self.position}, {self.gain}")
                            self.flatten(self.gain, "GAIN_FLATTEN")
                            self.signalFlip = 0
                        elif self.bid > self.loss:
                            logging.info(f"LOSS HIT: {-self.position}, {self.loss}")
                            if abs(self.position) != 1:
                                self.signalFlip = 0
                            self.flatten(self.loss, "LOSS_FLATTEN")

                    # EVERY TOP OF THE MINUTE...
                    if self.currentTime > self.lastTime + timedelta(seconds=60):
                        logging.info(f"Contract: {self.product}")
                        logging.info(f"Bid: {self.bid} | Ask: {self.ask}")
                        if self.gainOrder is not None and self.lossOrder is not None:
                            logging.info(
                                f"Gain: {self.gainOrder.orderNo} | Loss: {self.lossOrder.orderNo}"
                            )
                        else:
                            logging.info(
                                f"Gain: {self.gainOrder} | Loss: {self.lossOrder}"
                            )
                        if self.lastPrice is None:
                            self.lastPrice = self.latestTradePrice
                            logging.info(
                                f"Established Latest Price: {self.latestTradePrice}"
                            )
                        else:
                            move = self.latestTradePrice - self.lastPrice
                            self.add_votes(move, toPrint=self.toPrint)
                            self.check_votes_for_final_votes(toPrint=self.toPrint)
                            self.check_signal_from_final_votes(toPrint=self.toPrint)
                            self.lastPrice = self.latestTradePrice
                        logging.info(f"Latest Price: {self.latestTradePrice}")

                        # DIFFERENT LOGIC BASED ON DIFFERENT POSITIONS
                        # signal flip, if we have any position, flatten
                        if self.position * self.signal <= 0 and self.position != 0:
                            if self.position > 0:
                                self.flatten(self.bid, "SIGNAL_FLATTEN")
                            if self.position < 0:
                                self.flatten(self.ask, "SIGNAL_FLATTEN")

                        # start trade if position 0 and signalFlip is reset
                        elif self.position == 0:
                            if self.signal == -1 and self.signalFlip != self.signal:
                                self.order(self.bid, -1, tag="START_SELL")
                                self.position = -1
                                self.signalFlip = -1
                                self.entryPrices = [self.bid]
                                self.initialTradeTime = self.currentTime.replace(
                                    second=0, microsecond=0
                                )
                                self.gain = self.tickRound(
                                    self.latestTradePrice * (1 - self.gainLimit)
                                )
                                self.order(price=self.gain, qty=1, worker="G")
                                self.loss = self.tickRound(
                                    self.latestTradePrice * (1 + self.lossLimit)
                                )
                                self.order(
                                    price=self.loss - self.ticksAway, qty=1, worker="D"
                                )
                            elif self.signal == 1 and self.signalFlip != self.signal:
                                self.order(self.ask, 1, tag="START_BUY")
                                self.position = 1
                                self.signalFlip = 1
                                self.entryPrices = [self.ask]
                                self.initialTradeTime = self.currentTime.replace(
                                    second=0, microsecond=0
                                )
                                self.gain = self.tickRound(
                                    self.latestTradePrice * (1 + self.gainLimit)
                                )
                                self.order(price=self.gain, qty=-1, worker="G")
                                self.loss = self.tickRound(
                                    self.latestTradePrice * (1 - self.lossLimit)
                                )
                                self.order(
                                    price=self.loss + self.ticksAway, qty=-1, worker="D"
                                )

                        # position is positive, do our adding checks
                        elif self.position > 0:
                            if self.currentTime > self.initialTradeTime + timedelta(
                                seconds=self.check4 * 60
                            ):
                                self.pnlCheckCounter = 5
                                if self.pnlCheckCounter != self.position:
                                    logging.info(
                                        f"Entry Prices: {self.entryPrices}, Latest: {self.latestTradePrice}"
                                    )
                                    if (
                                        np.mean(self.entryPrices)
                                        > self.latestTradePrice
                                    ):
                                        self.signalFlip = 0
                                        self.flatten(self.bid, "PNL5_FLATTEN")
                                    elif self.position < 5:
                                        last_pos = self.position
                                        self.position += 1
                                        self.adjustGainLoss(5)
                                        self.order(
                                            self.ask,
                                            self.position - last_pos,
                                            tag="ADD5_BUY",
                                        )
                                        self.entryPrices.append(self.latestTradePrice)
                            elif self.currentTime > self.initialTradeTime + timedelta(
                                seconds=self.check3 * 60
                            ):
                                self.pnlCheckCounter = 4
                                if self.pnlCheckCounter != self.position:
                                    logging.info(
                                        f"Entry Prices: {self.entryPrices}, Latest: {self.latestTradePrice}"
                                    )
                                    if (
                                        np.mean(self.entryPrices)
                                        > self.latestTradePrice
                                    ):
                                        self.signalFlip = 0
                                        self.flatten(self.bid, "PNL4_FLATTEN")
                                    elif self.position < 4:
                                        last_pos = self.position
                                        self.position += 1
                                        self.adjustGainLoss(4)
                                        self.order(
                                            self.ask,
                                            self.position - last_pos,
                                            tag="ADD4_BUY",
                                        )
                                        self.entryPrices.append(self.latestTradePrice)
                            elif self.currentTime > self.initialTradeTime + timedelta(
                                seconds=self.check2 * 60
                            ):
                                self.pnlCheckCounter = 3
                                if self.pnlCheckCounter != self.position:
                                    logging.info(
                                        f"Entry Prices: {self.entryPrices}, Latest: {self.latestTradePrice}"
                                    )
                                    if (
                                        np.mean(self.entryPrices)
                                        > self.latestTradePrice
                                    ):
                                        self.signalFlip = 0
                                        self.flatten(self.bid, "PNL3_FLATTEN")
                                    elif self.position < 3:
                                        last_pos = self.position
                                        self.position += 1
                                        self.adjustGainLoss(3)
                                        self.order(
                                            self.ask,
                                            self.position - last_pos,
                                            tag="ADD3_BUY",
                                        )
                                        self.entryPrices.append(self.latestTradePrice)
                            elif self.currentTime > self.initialTradeTime + timedelta(
                                seconds=self.check1 * 60
                            ):
                                self.pnlCheckCounter = 2
                                if self.pnlCheckCounter != self.position:
                                    logging.info(
                                        f"Entry Prices: {self.entryPrices}, Latest: {self.latestTradePrice}"
                                    )
                                    if (
                                        np.mean(self.entryPrices)
                                        > self.latestTradePrice
                                    ):
                                        self.flatten(self.bid, "PNL2_FLATTEN")
                                    elif self.position < 2:
                                        last_pos = self.position
                                        self.position += 1
                                        self.adjustGainLoss(2)
                                        self.order(
                                            self.ask,
                                            self.position - last_pos,
                                            tag="ADD2_BUY",
                                        )
                                        self.entryPrices.append(self.latestTradePrice)

                        # position is negative, do our adding checks
                        elif self.position < 0:
                            if self.currentTime > self.initialTradeTime + timedelta(
                                seconds=self.check4 * 60
                            ):
                                self.pnlCheckCounter = -5
                                if self.pnlCheckCounter != self.position:
                                    logging.info(
                                        f"Entry Prices: {self.entryPrices}, Latest: {self.latestTradePrice}"
                                    )
                                    if (
                                        np.mean(self.entryPrices)
                                        < self.latestTradePrice
                                    ):
                                        self.signalFlip = 0
                                        self.flatten(self.ask, "PNL5_FLATTEN")
                                    elif self.position > -5:
                                        last_pos = self.position
                                        self.position -= 1
                                        self.adjustGainLoss(5)
                                        self.order(
                                            self.bid,
                                            self.position - last_pos,
                                            tag="ADD5_SELL",
                                        )
                                        self.entryPrices.append(self.latestTradePrice)
                            elif self.currentTime > self.initialTradeTime + timedelta(
                                seconds=self.check3 * 60
                            ):
                                self.pnlCheckCounter = -4
                                if self.pnlCheckCounter != self.position:
                                    logging.info(
                                        f"Entry Prices: {self.entryPrices}, Latest: {self.latestTradePrice}"
                                    )
                                    if (
                                        np.mean(self.entryPrices)
                                        < self.latestTradePrice
                                    ):
                                        self.signalFlip = 0
                                        self.flatten(self.ask, "PNL4_FLATTEN")
                                    elif self.position > -4:
                                        last_pos = self.position
                                        self.position -= 1
                                        self.adjustGainLoss(4)
                                        self.order(
                                            self.bid,
                                            self.position - last_pos,
                                            tag="ADD4_SELL",
                                        )
                                        self.entryPrices.append(self.latestTradePrice)
                            elif self.currentTime > self.initialTradeTime + timedelta(
                                seconds=self.check2 * 60
                            ):
                                self.pnlCheckCounter = -3
                                if self.pnlCheckCounter != self.position:
                                    logging.info(
                                        f"Entry Prices: {self.entryPrices}, Latest: {self.latestTradePrice}"
                                    )
                                    if (
                                        np.mean(self.entryPrices)
                                        < self.latestTradePrice
                                    ):
                                        self.signalFlip = 0
                                        self.flatten(self.ask, "PNL3_FLATTEN")
                                    elif self.position > -3:
                                        last_pos = self.position
                                        self.position -= 1
                                        self.adjustGainLoss(3)
                                        self.order(
                                            self.bid,
                                            self.position - last_pos,
                                            tag="ADD3_SELL",
                                        )
                                        self.entryPrices.append(self.latestTradePrice)
                            elif self.currentTime > self.initialTradeTime + timedelta(
                                seconds=self.check1 * 60
                            ):
                                self.pnlCheckCounter = -2
                                if self.pnlCheckCounter != self.position:
                                    logging.info(
                                        f"Entry Prices: {self.entryPrices}, Latest: {self.latestTradePrice}"
                                    )
                                    if (
                                        np.mean(self.entryPrices)
                                        < self.latestTradePrice
                                    ):
                                        self.flatten(self.ask, "PNL2_FLATTEN")
                                    elif self.position > -2:
                                        last_pos = self.position
                                        self.position -= 1
                                        self.adjustGainLoss(2)
                                        self.order(
                                            self.bid,
                                            self.position - last_pos,
                                            tag="ADD2_SELL",
                                        )
                                        self.entryPrices.append(self.latestTradePrice)

                        # update our last time
                        self.lastTime = self.currentTime.replace(
                            second=0, microsecond=0
                        )

                        # add a sleep for CPU
                        time.sleep(0.1)

    # region HELPERS
    def order(
        self,
        price,
        qty,
        worker="w",
        account=None,
        product=None,
        exchange=1,
        tag="",
    ):
        if account is None:
            account = self.account
        if product is None:
            product = self.product
        clients.tradingClient.order(
            account=self.account,
            product=self.product,
            price=price * self.productDiv,
            qty=qty,
            worker=worker,
            exchange=exchange,
            tag=tag,
        )

    def changeOrder(
        self,
        orderNo,
        price,
        qty,
        worker="D",
        account=None,
        product=None,
        exchange=1,
    ):
        if account is None:
            account = self.account
        if product is None:
            product = self.product
        clients.tradingClient.change_order(
            orderNo=orderNo,
            price=price * self.productDiv,
            qty=qty,
            worker=worker,
            account=account,
            product=product,
            exchange=exchange,
        )

    def adjustGainLoss(self, qty):
        if self.gainOrder is not None and self.lossOrder is not None:
            self.changeOrder(
                self.gainOrder.orderNo, self.gainOrder.price, qty, self.gainOrder.prefix
            )
            self.changeOrder(
                self.lossOrder.orderNo, self.lossOrder.price, qty, self.lossOrder.prefix
            )
        else:
            logging.info(
                f"Gain or Loss order not found. Gain: {self.gainOrder}, Loss: {self.lossOrder}"
            )

    def cancelGainLoss(self):
        if self.gainOrder is not None and self.lossOrder is not None:
            clients.tradingClient.cancel_order(self.gainOrder.orderNo)
            clients.tradingClient.cancel_order(self.lossOrder.orderNo)
            self.gainOrder = None
            self.lossOrder = None
        else:
            logging.info(
                f"Gain or Loss order not found. Gain: {self.gainOrder}, Loss: {self.lossOrder}"
            )

    def flatten(self, price, tag):
        if self.position != 0:
            clients.tradingClient.flatten(
                self.account,
                self.product,
                price * self.productDiv,
                -self.position,
                worker="w",
                exchange=1,
                tag=tag,
            )
        else:
            logging.info("Flatten attempted, position already 0")
        self.initialTradeTime = None
        self.cancelGainLoss()
        self.gain = None
        self.gainOrder = None
        self.loss = None
        self.lossOrder = None
        self.entryPrices = []
        self.pnlCheckCounter = 0
        self.position = 0

    def add_votes(self, move, toPrint=False):
        if not isinstance(move, (int, float)):
            logging.info(f"Move given is not valid: {move}")
            return
        if np.isnan(move):
            logging.info(f"Move given is NA: {move}")
            return
        to_add = int(move / self.voteDiv)
        if to_add > 0:
            for i in np.arange(to_add):
                self.votes.append(1)
        elif to_add < 0:
            for i in np.arange(abs(to_add)):
                self.votes.append(-1)
        self.voteTotal = sum(self.votes)
        percent_to_show = 0.5 + (self.voteTotal / (2 * self.voteCount))
        if percent_to_show < 0.5:
            percent_to_show = -1 * (1 - percent_to_show)
        percent_to_show = 100 * percent_to_show
        percent_to_show = round(percent_to_show, 1)
        if toPrint:
            logging.info(
                f"Change: {move}, Votes: {to_add}, Total Vote: {percent_to_show}%"
            )

    def check_votes_for_final_votes(self, toPrint=False):
        if len(self.votes) == self.voteCount and not self.votesFull:
            if toPrint:
                logging.info("Votes Full")
            self.votesFull = True
        if self.votesFull:
            if self.voteTotal > 0:
                self.finalVotes.append(1)
            elif self.voteTotal < 0:
                self.finalVotes.append(-1)
            else:
                self.finalVotes.append(0)

    def check_signal_from_final_votes(self, toPrint=False):
        if sum(self.finalVotes) == self.finalVotesCount:
            self.signal = 1
            if toPrint:
                logging.info(
                    f"FinalVotes: {list(self.finalVotes)}, Signal: {self.signal}"
                )
        elif sum(self.finalVotes) == -1 * self.finalVotesCount:
            self.signal = -1
            if toPrint:
                logging.info(
                    f"FinalVotes: {list(self.finalVotes)}, Signal: {self.signal}"
                )
        else:
            if self.signalFlip != 0:
                self.signalFlip = 0
            self.signal = 0
            if toPrint:
                logging.info(
                    f"FinalVotes: {list(self.finalVotes)}, Signal: {self.signal}"
                )

    def tickRound(self, num, div=4):
        return round(num * div) / div

    def disconnect(self):
        self.running = False
