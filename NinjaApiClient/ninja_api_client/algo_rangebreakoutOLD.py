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


class RB(Algo):
    def __init__(self):
        # CONSTANT PARAMETERS
        self.account = "FW078"
        self.product = "NQU5"
        self.productDiv = 100
        self.volumeThreshold = 100_000
        self.rangeThreshold = 150
        self.volThreshold = 0.0025
        self.volMaxThreshold = 0.002
        self.tickMinThreshold = 4
        self.volFreq = 600
        self.buyStartThreshold = 25
        self.buyStopThreshold = 200
        self.sellStartThreshold = 25
        self.sellStopThreshold = 100
        self.marketStop = datetime.now(ZoneInfo("America/Chicago")).replace(
            hour=15, minute=59, second=0, microsecond=0
        )

        # DYNAMIC PARAMETERS
        self.volume = 0
        self.volume_init = 0
        self.volume_add = 0
        self.rangeHigh = None
        self.rangeLow = None
        self.range = None
        self.highReset = True
        self.lowReset = True
        self.vol = 0
        self.volumeOK = False
        self.volOK = False
        self.rangeOK = False
        self.windowOpen = None
        self.windowHigh = None
        self.windowLow = None
        self.windowClose = None
        self.buyStart = None
        self.buyStop = None
        self.sellStart = None
        self.sellStop = None
        self.currentTime = None
        self.lastVolCheck = None
        self.running = True
        self.printTime = None
        self.latestTradePrice = None
        self.bid = None
        self.ask = None
        self.position = 0

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
            LIMIT 500_000
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
        now = datetime.now(ZoneInfo("America/Chicago"))
        self.printTime = now.replace(second=0, microsecond=0)
        if now.hour >= 16:
            start_time = now.replace(hour=17, minute=0, second=0, microsecond=0)
        else:
            start_time = (now - timedelta(days=1)).replace(
                hour=17, minute=0, second=0, microsecond=0
            )
        df["t_price"] = df["t_price"] / self.productDiv
        mask = (df.index >= start_time) & (df.index <= now)
        df = df[mask].copy()
        self.volume_init = df["t_qty"].sum()
        low = df["t_price"].min()
        high = df["t_price"].max()
        self.rangeHigh = high
        self.rangeLow = low
        self.range = self.rangeHigh - self.rangeLow
        logging.info(f"Volume: {self.volume_init}")
        logging.info(f"Range: {self.range}")
        minutes = (now.minute // 10) * 10
        start_time = now.replace(minute=minutes, second=0, microsecond=0) - timedelta(
            seconds=self.volFreq
        )
        end_time = now.replace(minute=minutes, second=0, microsecond=0)
        mask = (df.index >= start_time) & (df.index <= end_time)
        df1 = df[mask].copy()
        self.windowOpen = df1["t_price"].iloc[0]
        self.windowHigh = df1["t_price"].max()
        self.windowLow = df1["t_price"].min()
        self.windowClose = df1["t_price"].iloc[-1]
        self.vol = self.garmanKlass()
        if self.vol < self.volThreshold:
            logging.info(f"volOK: {self.vol:.5f}, {self.volThreshold}")
            self.volOK = True
        else:
            logging.info(f"NOT volOK: {self.vol:.5f}, {self.volThreshold}")
        mask = (df.index >= end_time) & (df.index <= now)
        df2 = df[mask].copy()
        if len(df2) > 0:
            self.windowOpen = df2["t_price"].iloc[0]
            self.windowHigh = df2["t_price"].max()
            self.windowLow = df2["t_price"].min()
            self.windowClose = None
        else:
            self.windowOpen = df1["t_price"].iloc[-1]
            self.windowHigh = df1["t_price"].iloc[-1]
            self.windowLow = df1["t_price"].iloc[-1]
            self.windowClose = None
        self.lastVolCheck = end_time
        logging.info(
            f"WARMUP DONE: {self.windowOpen}, {self.windowHigh}, {self.windowLow}, {self.vol:.5f}"
        )

    def run(self):
        self.warmup()
        while self.running:
            # set vars
            self.currentTime = datetime.now(ZoneInfo("America/Chicago"))
            self.bid = clients.tradingClient.get_bid(self.product) / self.productDiv
            self.ask = clients.tradingClient.get_ask(self.product) / self.productDiv
            self.latestTradePrice = (
                clients.tradingClient.get_trade_price(self.product) / self.productDiv
            )
            self.windowHigh = max(
                self.windowHigh,
                clients.tradingClient.get_high(self.product) / self.productDiv,
            )
            self.windowLow = min(
                self.windowLow,
                clients.tradingClient.get_low(self.product) / self.productDiv,
            )

            # check volume and range thresholds
            self.volume_add = clients.tradingClient.get_volume(self.product)
            self.volume = self.volume_init + self.volume_add
            if not self.volumeOK and self.volume >= self.volumeThreshold:
                logging.info(f"VolumeOK: {self.volume}")
                self.volumeOK = True
            if (
                clients.tradingClient.get_high(self.product) / self.productDiv
                > self.rangeHigh
            ):
                self.rangeHigh = (
                    clients.tradingClient.get_high(self.product) / self.productDiv
                )
                self.range = self.rangeHigh - self.rangeLow
                self.highReset = True
            if (
                clients.tradingClient.get_low(self.product) / self.productDiv
                < self.rangeLow
            ):
                self.rangeLow = (
                    clients.tradingClient.get_low(self.product) / self.productDiv
                )
                self.range = self.rangeHigh - self.rangeLow
                self.lowReset = True
            if self.range > self.rangeThreshold and not self.rangeOK:
                logging.info(
                    f"RangeOK: {self.range}, {self.rangeHigh}, {self.rangeLow}"
                )
                self.rangeOK = True

            if self.lastVolCheck + timedelta(seconds=self.volFreq) < self.currentTime:
                self.windowClose = self.latestTradePrice
                self.vol = self.garmanKlass()
                self.windowClose = None
                self.windowOpen = self.latestTradePrice
                self.windowHigh = self.latestTradePrice
                self.windowLow = self.latestTradePrice
                if self.vol < self.volThreshold:
                    logging.info(f"VolOK: {self.vol:.5f}, {self.volThreshold}")
                    self.volOK = True
                else:
                    logging.info(f"NOT VolOK: {self.vol:.5f}, {self.volThreshold}")
                self.lastVolCheck = self.currentTime.replace(second=0, microsecond=0)

            self.calcLevels()
            if self.currentTime > self.printTime + timedelta(seconds=60):
                logging.info(
                    f"High: {self.rangeHigh}, Low: {self.rangeLow}, Volume: {self.volume}, Vol: {self.vol:.5f}\nLevels: [{self.sellStart}, {self.sellStop}, {self.buyStop}, {self.buyStart}]"
                )
                self.printTime = self.printTime + timedelta(seconds=60)

            if self.volumeOK and self.rangeOK:
                if self.volOK:
                    if self.position == 0:
                        if (
                            self.bid >= self.buyStart
                            or self.latestTradePrice >= self.buyStart
                        ):
                            self.order(self.buyStart, 1, tag="START_BUY")
                            self.position += 1
                        elif (
                            self.ask <= self.sellStart
                            or self.latestTradePrice <= self.sellStart
                        ):
                            self.order(self.sellStart, -1, tag="START_SELL")
                            self.position -= 1

                if self.position > 0:
                    if (
                        self.ask <= self.buyStop
                        or self.latestTradePrice <= self.buyStop
                    ):
                        self.order(self.buyStop, -1, tag="STOP_BUY")
                        self.position -= 1
                        self.highReset = False
                if self.position < 0:
                    if (
                        self.bid >= self.sellStop
                        or self.latestTradePrice >= self.sellStop
                    ):
                        self.order(self.sellStop, 1, tag="STOP_SELL")
                        self.position += 1
                        self.lowReset = False

            if self.currentTime >= self.marketStop:
                logging.info(f"RB: Outside of Market Hours, flattening...")
                self.flatten(self.latestTradePrice, "MARKET_FLATTEN")
                self.disconnect()

            # add a sleep for CPU
            time.sleep(0.1)

    # region HELPERS
    def order(
        self,
        price,
        qty,
        worker="w",
        exchange=1,
        tag="",
        log=True,
    ):
        clients.tradingClient.order(
            account=self.account,
            product=self.product,
            price=price * self.productDiv,
            qty=qty,
            worker=worker,
            exchange=exchange,
            tag=tag,
            log=log,
        )

    def flatten(self, price, tag, log=True):
        if self.position != 0:
            clients.tradingClient.flatten(
                self.account,
                self.product,
                price * self.productDiv,
                -self.position,
                worker="w",
                exchange=1,
                tag=tag,
                log=log,
            )
        else:
            logging.info("Flatten attempted, position already 0")
        self.position = 0

    # RECALCULATE LEVELS
    def calcLevels(self):
        self.buyStart = self.tickRound(
            (
                self.rangeHigh
                - max(
                    self.tickMinThreshold,
                    (
                        self.range
                        * min(self.volMaxThreshold, self.vol)
                        * self.buyStartThreshold
                    ),
                )
            ),
            4,
        )
        self.buyStop = self.tickRound(
            (
                self.rangeHigh
                - max(
                    self.tickMinThreshold,
                    (
                        self.range
                        * min(self.volMaxThreshold, self.vol)
                        * self.buyStopThreshold
                    ),
                )
            ),
            4,
        )
        self.sellStop = self.tickRound(
            (
                self.rangeLow
                + max(
                    self.tickMinThreshold,
                    (
                        self.range
                        * min(self.volMaxThreshold, self.vol)
                        * self.sellStopThreshold
                    ),
                )
            ),
            4,
        )
        self.sellStart = self.tickRound(
            (
                self.rangeLow
                + max(
                    self.tickMinThreshold,
                    (
                        self.range
                        * min(self.volMaxThreshold, self.vol)
                        * self.sellStartThreshold
                    ),
                )
            ),
            4,
        )

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
