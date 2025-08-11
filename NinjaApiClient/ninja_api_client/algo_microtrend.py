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
from zoneinfo import ZoneInfo
import pandas as pd
import numpy as np


class MT(Algo):
    def __init__(self):
        # CONSTANT PARAMETERS
        self.account = "FW079"
        self.product = "NQU5"
        self.exchange = 1
        self.productDiv = 100
        self.maxWidth = 4
        self.trail = 0.0025
        self.ticksAway = 1_000

        # self.volumeThreshold = 1_000
        # self.rangeThreshold = 150
        # self.volThreshold = 0.0025
        # self.volMaxThreshold = 0.002
        # self.tickMinThreshold = 4
        # self.volFreq = 600
        # self.buyStartThreshold = 25
        # self.buyStopThreshold = 200
        # self.sellStartThreshold = 25
        # self.sellStopThreshold = 100
        # self.marketStop = datetime.now(ZoneInfo("America/Chicago")).replace(
        #     hour=15, minute=59, second=0, microsecond=0
        # )

        # DYNAMIC PARAMETERS
        self.running = True
        self.dir = 0
        self.secLatestTradePrice = None
        self.latestTradePrice = None
        self.bid = None
        self.ask = None
        self.activeOrder = None
        self.lastTrailPrice = None
        self.trailLevel = None
        self.flipped = False
        self.positionCounter = clients.positionsClient.positionCounter
        self.tooWide = False

        # self.volume = 0
        # self.volume_init = 0
        # self.volume_add = 0
        # self.rangeHigh = None
        # self.rangeLow = None
        # self.range = None
        # self.highReset = True
        # self.lowReset = True
        # self.vol = 0
        # self.volumeOK = False
        # self.volOK = False
        # self.rangeOK = False
        # self.windowOpen = None
        # self.windowHigh = None
        # self.windowLow = None
        # self.windowClose = None
        # self.buyStart = None
        # self.buyStop = None
        # self.sellStart = None
        # self.sellStop = None
        # self.oldLevels = [None, None, None, None]
        # self.currentTime = None
        # self.lastVolCheck = None
        # self.printTime = None
        # self.latestTradePrice = None
        # self.bid = None
        # self.ask = None
        # self.position = 0
        # self.activeOrders = {"BUY": None, "SELL": None}

    def warmup(self):
        engine = create_engine(
            "postgresql+psycopg2://tickreader:tickreader@tsdb:5432/cme",
            poolclass=NullPool,  # don't reuse connections — safe for scripts
            connect_args={"connect_timeout": 5},
        )
        query = f"""
            SELECT wh_name, t_price, t_qty, sending_time
            FROM nq_fut_trades_weekly
            WHERE wh_name = %s
            ORDER BY sending_time DESC
            LIMIT 50_000
        """
        try:
            with engine.connect() as conn:
                df = pd.read_sql(query, conn, params=(self.product,))
        except Exception as e:
            print("Error:", e)
        finally:
            engine.dispose()
        df.drop(columns=["wh_name"], inplace=True)
        if len(df) < 50_000:
            logging.error("MT: DATA REQUEST SEEMS OFF")
        df["sending_time"] = df["sending_time"].str.replace(
            r"(\.\d{6})\d+", r"\1", regex=True
        )
        df["sending_time"] = pd.to_datetime(
            df["sending_time"], format="%Y-%m-%d %H:%M:%S.%f %z"
        )
        df.index = df["sending_time"]
        df = df.sort_index()
        now = datetime.now(ZoneInfo("America/Chicago"))
        start_time = now - timedelta(seconds=900)
        df["t_price"] = df["t_price"] / self.productDiv
        mask = (df.index >= start_time) & (df.index <= now)
        df = df[mask].copy()
        if len(df) < 2:
            logging.error("MT: WARMUP LENGTH LESS THAN 2")
            self.disconnect()
            return
        self.dir = np.sign(df.iloc[-1]["t_price"] - df.iloc[0]["t_price"])
        if self.dir == 0:
            self.dir = 1
            self.latestTradePrice = df.iloc[-1]["t_price"]
            self.lastTraip
        logging.info(
            f"MT, WARMUP DONE: {self.dir}, {df.iloc[-1]['t_price']}, {df.iloc[0]['t_price']}"
        )

    def run(self):
        self.warmup()
        while self.running:
            # set vars and query for position
            self.currentTime = datetime.now(ZoneInfo("America/Chicago"))
            self.bid = clients.tradingClient.get_bid(self.product)
            self.ask = clients.tradingClient.get_ask(self.product)
            self.secLatestTradePrice = self.latestTradePrice
            self.latestTradePrice = clients.tradingClient.get_trade_price(self.product)

            # get active order
            orders_snapshot = list(clients.tradingClient.activeOrders.values())
            for order in orders_snapshot:
                if order.account == self.account:
                    self.activeOrder = order
                    break

            # do divisor adjustment for prices
            if any(x is None for x in [self.bid, self.ask, self.latestTradePrice]):
                continue
            self.bid = self.bid / self.productDiv
            self.ask = self.ask / self.productDiv
            if self.ask - self.bid > self.maxWidth:
                self.tooWide = True
            else:
                self.tooWide = False
            self.latestTradePrice = self.latestTradePrice / self.productDiv

            # get updated position
            while self.positionCounter == clients.positionsClient.positionCounter:
                time.sleep(0.01)
            self.positionCounter = clients.positionsClient.positionCounter
            if (
                self.position
                == -1
                * clients.positionsClient.positions[
                    (
                        self.account,
                        self.exchange,
                        self.product,
                    )
                ]
            ):
                self.dir = np.sign(self.position)
                self.flipped = True

            # if position is 0 and we have a direction, start the trade if market is not too wide
            if self.position == 0:
                if self.dir > 0 and not self.tooWide:
                    self.order(self.ask, 1, tag="START_BUY")
                    self.lastTrailPrice = self.ask
                    self.position = 1
                    self.calcLevels()
                    continue
                elif self.dir < 0 and not self.tooWide:
                    self.order(self.bid, -1, tag="START_SELL")
                    self.lastTrailPrice = self.bid
                    self.position = -1
                    self.calcLevels()
                    continue

            self.calcLevels()
            if self.position < 0:
                if self.tooWide:
                    self.flatten(self.latestTradePrice, "MARKET_FLATTEN")
                    self.position = 0
            elif self.position > 0:
                if self.tooWide:
                    self.flatten(self.latestTradePrice, "MARKET_FLATTEN")
                    self.position = 0

            time.sleep(0.1)

    # region HELPERS
    # RECALCULATE LEVELS
    def calcLevels(self):
        if self.flipped:
            self.activeOrder = None
            self.flipped = False

        if self.position < 0:
            self.lastTrailPrice = min(
                clients.tradingClient.get_low(self.product) / self.productDiv,
                self.lastTrailPrice,
            )
            self.trailLevel = tickRound(1 + self.trail) * self.lastTrailPrice
            if self.activeOrder is None:
                self.order(
                    qty=2,
                    price=self.trailLevel - self.ticksAway,
                    worker="D",
                )
            elif (
                self.trailLevel - self.ticksAway
                < self.activeOrder.price / self.productDiv
            ):
                self.changeOrder(
                    orderNo=self.activeOrder.orderNo,
                    price=self.trailLevel - self.ticksAway,
                    qty=self.activeOrder.qty,
                )
        elif self.position > 0:
            self.lastTrailPrice = max(
                clients.tradingClient.get_high(self.product) / self.productDiv,
                self.lastTrailPrice,
            )
            self.trailLevel = tickRound(1 - self.trail) * self.lastTrailPrice
            if self.activeOrder is None:
                self.order(
                    qty=-2,
                    price=self.trailLevel + self.ticksAway,
                    worker="D",
                )
            elif (
                self.trailLevel + self.ticksAway
                > self.activeOrder.price / self.productDiv
            ):
                self.changeOrder(
                    orderNo=self.activeOrder.orderNo,
                    price=self.trailLevel + self.ticksAway,
                    qty=self.activeOrder.qty,
                )

    def order(
        self,
        price,
        qty,
        worker="w",
        exchange=1,
        tag="",
    ):
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

    def flatten(self, price, tag):
        # if self.position != 0:
        clients.tradingClient.flatten(
            self.account,
            self.product,
            price * self.productDiv,
            -self.position,
            worker="w",
            exchange=1,
            tag=tag,
        )
        # else:
        #     logging.info("Flatten attempted, position already 0")
        self.position = 0

    def garmanKlass(self, log=True):
        if all(
            x is not None
            for x in [
                self.windowHigh,
                self.windowLow,
                self.windowClose,
                self.windowOpen,
            ]
        ):
            log_high_low = np.log(self.windowHigh / self.windowLow) ** 2
            log_close_open = np.log(self.windowClose / self.windowOpen) ** 2
            gk = np.sqrt(0.5 * log_high_low - (2 * np.log(2) - 1) * log_close_open)
            gk = np.log1p(gk)
            if log:
                logging.info(
                    f"Vol Calc: {gk:.5f}, {self.windowOpen}, {self.windowHigh}, {self.windowLow}, {self.windowClose}"
                )
            return gk
        else:
            return None

    def tickRound(self, num, div=4):
        return round(num * div) / div

    def disconnect(self):
        self.running = False
