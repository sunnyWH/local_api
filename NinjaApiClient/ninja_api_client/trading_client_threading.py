from ninja_api_client import NinjaApiClient
from config import settings

import NinjaApiWorkingRules_pb2
import NinjaApiMarketData_pb2
import NinjaApiContracts_pb2
import NinjaApiMessages_pb2
import NinjaApiSheets_pb2
import NinjaApiCommon_pb2

import logging
import time

from HELPERS import TradingLogger

# USER IMPORTS
from datetime import time as datetimeTime
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from collections import deque
import pandas as pd
import numpy as np
import threading


class TradingClient(NinjaApiClient):

    def __init__(self):
        super().__init__(settings.trading_host, settings.trading_port)
        # CONSTANT PARAMETERS
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
        self.marketEnd = datetimeTime(14, 59)
        self.logger = TradingLogger()
        self.toPrint = True

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
        self.inMarket = None
        self.position = 0
        self.entryPrices = None
        self.initialTradeTime = None
        self.gain = None
        self.loss = None
        self.pnlCheckCounter = 0

    def user_quits(self):
        while True:
            if input("Type 'q' to quit: ").strip().lower() == "q":
                logging.info("Quit signal received.")
                container = NinjaApiMessages_pb2.MsgContainer()
                stopmd = NinjaApiMarketData_pb2.StopMarketData()
                container.header.msgType = (
                    NinjaApiMessages_pb2.Header.STOP_MARKET_DATA_REQUEST
                )
                self.flatten(self.lastPrice, "PROGRAM_FLATTEN")
                logging.info("Stopping market connection...")
                container.payload = stopmd.SerializeToString()
                self.send_msg(container)
                logging.info("Disconnecting...")
                self.disconnect()

    def trade_logic(self):
        while True:
            while self.inMarket:
                # EVERY TOP OF THE MINUTE...
                self.currentTime = datetime.now()
                if self.currentTime > self.lastTime + timedelta(seconds=60):
                    logging.info(f"Contract: {self.product}")
                    logging.info(f"Bid: {self.bid} | Ask: {self.ask}")
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
                            self.logger.log_trade(
                                self.currentTime.time(),
                                self.bid,
                                -self.position,
                                "EXIT-SIGNAL",
                            )
                            self.position = 0
                        if self.position < 0:
                            self.logger.log_trade(
                                self.currentTime.time(),
                                self.ask,
                                -self.position,
                                "EXIT-SIGNAL",
                            )
                            self.position = 0

                    # start trade if position 0 and signalFlip is reset
                    elif self.position == 0:
                        if self.signal == -1 and self.signalFlip != self.signal:
                            self.logger.log_trade(
                                self.currentTime.time(),
                                self.bid,
                                -1,
                                "START_SELL",
                            )
                            self.position = -1
                            self.signalFlip = -1
                            self.entryPrices = [self.bid]
                            self.initialTradeTime = self.currentTime.replace(
                                second=0, microsecond=0
                            )
                            self.gain = self.tickRound(
                                self.latestTradePrice * (1 - self.gainLimit)
                            )
                            self.loss = self.tickRound(
                                self.latestTradePrice * (1 + self.lossLimit)
                            )
                        elif self.signal == 1 and self.signalFlip != self.signal:
                            self.logger.log_trade(
                                self.currentTime.time(),
                                self.ask,
                                1,
                                "START_BUY",
                            )
                            self.position = 1
                            self.signalFlip = 1
                            self.entryPrices = [self.ask]
                            self.initialTradeTime = self.currentTime.replace(
                                second=0, microsecond=0
                            )
                            self.gain = self.tickRound(
                                self.latestTradePrice * (1 + self.gainLimit)
                            )
                            self.loss = self.tickRound(
                                self.latestTradePrice * (1 - self.lossLimit)
                            )

                    # position is positive, do our adding checks
                    elif self.position > 0:
                        if self.currentTime > self.initialTradeTime + timedelta(
                            seconds=self.check4 * 60
                        ):
                            self.pnlCheckCounter = 5
                            if self.pnlCheckCounter != self.position:
                                if np.mean(self.entryPrices) > self.latestTradePrice:
                                    self.signalFlip = 0
                                    self.flatten(self.bid, "PNL5_FLATTEN")
                                elif self.position < 5:
                                    last_pos = self.position
                                    self.position += 1
                                    self.logger.log_trade(
                                        self.currentTime.time(),
                                        self.ask,
                                        self.position - last_pos,
                                        "ADD5_BUY",
                                    )
                        elif self.currentTime > self.initialTradeTime + timedelta(
                            seconds=self.check3 * 60
                        ):
                            self.pnlCheckCounter = 4
                            if self.pnlCheckCounter != self.position:
                                if np.mean(self.entryPrices) > self.latestTradePrice:
                                    self.signalFlip = 0
                                    self.flatten(self.bid, "PNL4_FLATTEN")
                                elif self.position < 4:
                                    last_pos = self.position
                                    self.position += 1
                                    self.logger.log_trade(
                                        self.currentTime.time(),
                                        self.ask,
                                        self.position - last_pos,
                                        "ADD4_BUY",
                                    )
                        elif self.currentTime > self.initialTradeTime + timedelta(
                            seconds=self.check2 * 60
                        ):
                            self.pnlCheckCounter = 3
                            if self.pnlCheckCounter != self.position:
                                if np.mean(self.entryPrices) > self.latestTradePrice:
                                    self.signalFlip = 0
                                    self.flatten(self.bid, "PNL3_FLATTEN")
                                elif self.position < 3:
                                    last_pos = self.position
                                    self.position += 1
                                    self.logger.log_trade(
                                        self.currentTime.time(),
                                        self.ask,
                                        self.position - last_pos,
                                        "ADD3_BUY",
                                    )
                        elif self.currentTime > self.initialTradeTime + timedelta(
                            seconds=self.check1 * 60
                        ):
                            self.pnlCheckCounter = 2
                            if self.pnlCheckCounter != self.position:
                                if np.mean(self.entryPrices) > self.latestTradePrice:
                                    self.flatten(self.bid, "PNL2_FLATTEN")
                                elif self.position < 2:
                                    last_pos = self.position
                                    self.position += 1
                                    self.logger.log_trade(
                                        self.currentTime.time(),
                                        self.ask,
                                        self.position - last_pos,
                                        "ADD2_BUY",
                                    )

                    # position is negative, do our adding checks
                    elif self.position < 0:
                        if self.currentTime > self.initialTradeTime + timedelta(
                            seconds=self.check4 * 60
                        ):
                            self.pnlCheckCounter = -5
                            if self.pnlCheckCounter != self.position:
                                if np.mean(self.entryPrices) < self.latestTradePrice:
                                    self.signalFlip = 0
                                    self.flatten(self.ask, "PNL5_FLATTEN")
                                elif self.position > -5:
                                    last_pos = self.position
                                    self.position -= 1
                                    self.logger.log_trade(
                                        self.currentTime.time(),
                                        self.bid,
                                        self.position - last_pos,
                                        "ADD5_SELL",
                                    )
                        elif self.currentTime > self.initialTradeTime + timedelta(
                            seconds=self.check3 * 60
                        ):
                            self.pnlCheckCounter = -4
                            if self.pnlCheckCounter != self.position:
                                if np.mean(self.entryPrices) < self.latestTradePrice:
                                    self.signalFlip = 0
                                    self.flatten(self.ask, "PNL4_FLATTEN")
                                elif self.position > -4:
                                    last_pos = self.position
                                    self.position -= 1
                                    self.logger.log_trade(
                                        self.currentTime.time(),
                                        self.bid,
                                        self.position - last_pos,
                                        "ADD4_SELL",
                                    )
                        elif self.currentTime > self.initialTradeTime + timedelta(
                            seconds=self.check2 * 60
                        ):
                            self.pnlCheckCounter = -3
                            if self.pnlCheckCounter != self.position:
                                if np.mean(self.entryPrices) < self.latestTradePrice:
                                    self.signalFlip = 0
                                    self.flatten(self.ask, "PNL3_FLATTEN")
                                elif self.position > -3:
                                    last_pos = self.position
                                    self.position -= 1
                                    self.logger.log_trade(
                                        self.currentTime.time(),
                                        self.bid,
                                        self.position - last_pos,
                                        "ADD3_SELL",
                                    )
                        elif self.currentTime > self.initialTradeTime + timedelta(
                            seconds=self.check1 * 60
                        ):
                            self.pnlCheckCounter = -2
                            if self.pnlCheckCounter != self.position:
                                if np.mean(self.entryPrices) < self.latestTradePrice:
                                    self.flatten(self.ask, "PNL2_FLATTEN")
                                elif self.position > -2:
                                    last_pos = self.position
                                    self.position -= 1
                                    self.logger.log_trade(
                                        self.currentTime.time(),
                                        self.bid,
                                        self.position - last_pos,
                                        "ADD2_SELL",
                                    )

                    # update our last time
                    self.lastTime = self.currentTime.replace(second=0, microsecond=0)

                    # start active waiting after 58.5 seconds
                    time.sleep(58.5)

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
        # df["sending_time"] = pd.to_datetime(df["sending_time"])
        df.index = df["sending_time"]
        df = df.sort_index()
        times = df.index.time
        mask = (times >= pd.to_datetime("08:30").time()) & (
            times < pd.to_datetime("15:00:03").time()
        )
        df = df[mask].copy()
        df["minute"] = df["sending_time"].dt.floor("min")
        # mask = df["sending_time"] > df["minute"]
        # df = df[mask]
        df = df.loc[df.groupby("minute")["sending_time"].idxmin()]  # last traded price
        df = df.drop(columns=["sending_time", "minute"])
        df["t_price"] = df["t_price"] / self.productDiv
        moves = df["t_price"].diff()
        time_diff = moves.index.to_series().diff()
        moves[time_diff > pd.Timedelta(minutes=2)] = 0
        moves.dropna(inplace=True)
        for move in moves:
            self.add_votes(move, toPrint=self.toPrint)
            self.check_votes_for_final_votes(toPrint=self.toPrint)
            self.check_signal_from_final_votes(toPrint=self.toPrint)

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
        if toPrint:
            logging.info(
                f"Change: {move}, Votes: {to_add}, Total Vote: {self.voteTotal}"
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

    def flatten(self, price, tag):
        if self.position != 0:
            self.logger.log_trade(
                self.currentTime.time(),
                price,
                -self.position,
                tag,
            )
        else:
            logging.info("Flatten attempted, position already 0")
        self.initialTradeTime = None
        self.gain = None
        self.loss = None
        self.pnlCheckCounter = 0
        self.position = 0

    def tickRound(self, num, div=4):
        return round(num * div) / div

    def run(self):
        ##________________________________________________________________________________
        ## WARMUP
        self.warmup()
        ##________________________________________________________________________________
        ## USER INPUT
        threading.Thread(target=self.user_quits, daemon=True).start()
        ##________________________________________________________________________________
        ## TRADE LOGIC
        threading.Thread(target=self.trade_logic, daemon=True).start()
        ##________________________________________________________________________________
        ## LOGIN
        login = NinjaApiMessages_pb2.Login()
        login.user = settings.trading_user
        login.password = settings.trading_password
        login.connectionType = NinjaApiMessages_pb2.ConnectionType.TRADING_CONNECTION
        login.accessToken = settings.trading_access_token
        container = NinjaApiMessages_pb2.MsgContainer()
        container.header.msgType = NinjaApiMessages_pb2.Header.LOGIN_REQUEST
        container.header.version = "v1.0.0"
        container.payload = login.SerializeToString()
        self.send_msg(container)
        ##________________________________________________________________________________
        ## REQUEST AVAILABLE FIELDS
        container.header.msgType = NinjaApiMessages_pb2.Header.NINJA_REQUEST
        container.payload = b""
        self.send_msg(container)
        container.header.msgType = NinjaApiMessages_pb2.Header.ACCOUNTS_REQUEST
        self.send_msg(container)
        container.header.msgType = NinjaApiMessages_pb2.Header.WORKING_RULES_REQUEST
        self.send_msg(container)
        container.header.msgType = NinjaApiMessages_pb2.Header.PRICE_FEED_STATUS_REQUEST
        self.send_msg(container)
        sheets = NinjaApiSheets_pb2.GetSheets()
        container.header.msgType = NinjaApiMessages_pb2.Header.SHEETS_REQUEST
        container.payload = sheets.SerializeToString()
        self.send_msg(container)
        ##________________________________________________________________________________
        ## START MARKET DATA
        startmd = NinjaApiMarketData_pb2.StartMarketData()
        contract = startmd.contracts.add()
        contract.exchange = NinjaApiCommon_pb2.Exchange.CME
        contract.secDesc = self.product
        contract.whName = self.product
        startmd.cadence.duration = 0  # in milliseconds
        startmd.includeImplieds = True
        startmd.includeTradeUpdates = True
        container.header.msgType = NinjaApiMessages_pb2.Header.START_MARKET_DATA_REQUEST
        container.payload = startmd.SerializeToString()
        self.send_msg(container)

        hb_count = 0
        while self.connected:
            # GET THE CORRECT TIMES
            if self.currentTime is None or not self.inMarket:
                self.currentTime = datetime.now()
            if (
                self.marketStart <= self.currentTime.time() <= self.marketEnd
                and not self.printed
            ):
                logging.info(f"In Market Hours")
                self.printed = True
                self.printedClosed = False
                self.inMarket = True
            if (
                not self.marketStart <= self.currentTime.time() <= self.marketEnd
                and not self.printedClosed
            ):
                logging.info(f"Outside of Market Hours, flattening...")
                self.flatten(self.latestTradePrice, "MARKET_FLATTEN")
                self.signalFlip = 0
                self.printed = False
                self.printedClosed = True
                self.inMarket = False

            if self.inMarket:
                if self.lastTime is None:
                    self.lastTime = self.currentTime.replace(second=0, microsecond=0)

            msg = self.recv_msg()
            if not msg:
                continue
            # logging.info(
            #     "Received "
            #     + NinjaApiMessages_pb2.Header.MsgType.Name(msg.header.msgType)
            # )
            if msg.header.msgType == NinjaApiMessages_pb2.Header.HEARTBEAT:
                hb_count += 1
                # if hb_count == 2:
                #     break  # Exit loop after receiving 3 heartbeats
            # ________________________________________________________________________________
            ## ON EVERY MARKET UPDATE
            elif msg.header.msgType == NinjaApiMessages_pb2.Header.MARKET_UPDATES:
                resp = NinjaApiMarketData_pb2.MarketUpdates()
                resp.ParseFromString(msg.payload)
                for update in resp.marketUpdates:
                    if len(update.tradeUpdates) > 0:
                        latestTrade = update.tradeUpdates[-1]
                        self.latestTradePrice = latestTrade.tradePrice / self.productDiv
                        self.bid = update.tobUpdate.bidPrice / self.productDiv
                        self.ask = update.tobUpdate.askPrice / self.productDiv
                        if self.inMarket:
                            # check gains and stops at every update on bid/ask
                            if self.position > 0:
                                if self.bid > self.gain:
                                    logging.info(
                                        f"GAIN HIT: {-self.position}, {self.gain}"
                                    )
                                    self.flatten(self.gain, "GAIN_FLATTEN")
                                    self.signalFlip = 0
                                elif self.ask < self.loss:
                                    logging.info(
                                        f"LOSS HIT: {-self.position}, {self.loss}"
                                    )
                                    if abs(self.position) != 1:
                                        self.signalFlip = 0
                                    self.flatten(self.loss, "LOSS_FLATTEN")
                            elif self.position < 0:
                                if self.ask < self.gain:
                                    logging.info(
                                        f"GAIN HIT: {-self.position}, {self.gain}"
                                    )
                                    self.flatten(self.gain, "GAIN_FLATTEN")
                                    self.signalFlip = 0
                                elif self.bid > self.loss:
                                    logging.info(
                                        f"LOSS HIT: {-self.position}, {self.loss}"
                                    )
                                    if abs(self.position) != 1:
                                        self.signalFlip = 0
                                    self.flatten(self.loss, "LOSS_FLATTEN")

            ##________________________________________________________________________________
            ## PRINT AVAILABLE FIELDS
            elif msg.header.msgType == NinjaApiMessages_pb2.Header.ERROR:
                error = NinjaApiMessages_pb2.Error()
                error.ParseFromString(msg.payload)
                logging.info(error.msg)
            elif msg.header.msgType == NinjaApiMessages_pb2.Header.NINJA_RESPONSE:
                resp = NinjaApiMessages_pb2.NinjaInfo()
                resp.ParseFromString(msg.payload)
                logging.info("Connected to ninja " + resp.name)
            elif msg.header.msgType == NinjaApiMessages_pb2.Header.ACCOUNTS_RESPONSE:
                resp = NinjaApiMessages_pb2.Accounts()
                resp.ParseFromString(msg.payload)
                logging.info("Available accounts are " + ", ".join(resp.accounts))
            elif msg.header.msgType == NinjaApiMessages_pb2.Header.SHEETS_RESPONSE:
                resp = NinjaApiSheets_pb2.Sheets()
                resp.ParseFromString(msg.payload)
                sheetnames = [sheet.name for sheet in resp.sheets]
                logging.info("Available sheets are " + ", ".join(sheetnames))
                getcontractinfo = NinjaApiContracts_pb2.GetContractInfo()
                getsettlements = NinjaApiContracts_pb2.GetSettlements()
                getsecuritystatuses = NinjaApiMarketData_pb2.GetSecurityStatuses()
                for sheet in resp.sheets:
                    logging.info(
                        f"Sheet {sheet.name} has contracts {', '.join(contract.secDesc for contract in sheet.contracts)}"
                    )
                    contracts = [
                        contract
                        for contract in sheet.contracts
                        if contract.secDesc != "-----"
                    ]
                    getcontractinfo.contracts.extend(contracts)
                    getsettlements.contracts.extend(contracts)
                    getsecuritystatuses.contracts.extend(contracts)
                if resp.sheets:
                    sheetrisk = NinjaApiSheets_pb2.GetSheetRisk()
                    sheetrisk.sheets.extend(sheetnames)
                    container.header.msgType = (
                        NinjaApiMessages_pb2.Header.SHEET_RISK_REQUEST
                    )
                    container.payload = sheetrisk.SerializeToString()
                    self.send_msg(container)
                    sheetstates = NinjaApiSheets_pb2.GetSheetStates()
                    sheetstates.sheets.extend(sheetnames)
                    container.header.msgType = (
                        NinjaApiMessages_pb2.Header.SHEET_STATE_REQUEST
                    )
                    container.payload = sheetstates.SerializeToString()
                    self.send_msg(container)
                    container.header.msgType = (
                        NinjaApiMessages_pb2.Header.CONTRACT_INFO_REQUEST
                    )
                    container.payload = getcontractinfo.SerializeToString()
                    self.send_msg(container)
                    container.header.msgType = (
                        NinjaApiMessages_pb2.Header.SETTLEMENTS_REQUEST
                    )
                    container.payload = getsettlements.SerializeToString()
                    self.send_msg(container)
                    container.header.msgType = (
                        NinjaApiMessages_pb2.Header.SECURITY_STATUSES_REQUEST
                    )
                    container.payload = getsecuritystatuses.SerializeToString()
                    self.send_msg(container)
            elif msg.header.msgType == NinjaApiMessages_pb2.Header.SHEET_RISK_RESPONSE:
                resp = NinjaApiSheets_pb2.SheetRiskList()
                resp.ParseFromString(msg.payload)
                for sheetrisk in resp.riskForSheets:
                    logging.info(
                        f"Sheet {sheetrisk.sheet} has clip size {sheetrisk.clipSize} and {sheetrisk.maxOrders - sheetrisk.ordersSent} order adds remaining"
                    )
            elif msg.header.msgType == NinjaApiMessages_pb2.Header.SHEET_STATE_RESPONSE:
                resp = NinjaApiSheets_pb2.SheetStates()
                resp.ParseFromString(msg.payload)
                for sheetstate in resp.sheetStates:
                    if (
                        sheetstate.status
                        == NinjaApiSheets_pb2.SheetState.Status.DISABLED
                    ):
                        logging.info(f"Sheet {sheetstate.sheet} is DISABLED")
                    elif sheetstate.status == NinjaApiSheets_pb2.SheetState.Status.OFF:
                        logging.info(f"Sheet {sheetstate.sheet} is OFF")
                    elif sheetstate.status == NinjaApiSheets_pb2.SheetState.Status.ON:
                        logging.info(f"Sheet {sheetstate.sheet} is ON")
            elif (
                msg.header.msgType == NinjaApiMessages_pb2.Header.CONTRACT_INFO_RESPONSE
            ):
                resp = NinjaApiContracts_pb2.ContractInfoList()
                resp.ParseFromString(msg.payload)
                for contractinfo in resp.contractInfoList:
                    logging.info(
                        f"Contract {contractinfo.contract.secDesc} has {len(contractinfo.legs)} legs. "
                        f"It ticks in {contractinfo.tickSize} increments and each tick is worth {contractinfo.tickAmt} {contractinfo.currency}."
                    )
            elif msg.header.msgType == NinjaApiMessages_pb2.Header.SETTLEMENTS_RESPONSE:
                resp = NinjaApiContracts_pb2.Settlements()
                resp.ParseFromString(msg.payload)
                for settlement in resp.settlements:
                    if settlement.HasField("prelim") and settlement.HasField("final"):
                        logging.info(
                            f"Contract {settlement.contract.secDesc} "
                            f"on {settlement.date.month}/{settlement.date.day}/{settlement.date.year} "
                            f"has prelim settlement {settlement.prelim} and final settlement {settlement.final}."
                        )
                    elif settlement.HasField("prelim"):
                        logging.info(
                            f"Contract {settlement.contract.secDesc} "
                            f"on {settlement.date.month}/{settlement.date.day}/{settlement.date.year} "
                            f"has prelim settlement {settlement.prelim}."
                        )
                    else:
                        logging.info(
                            f"Contract {settlement.contract.secDesc} "
                            f"on {settlement.date.month}/{settlement.date.day}/{settlement.date.year} "
                            f"has final settlement {settlement.final}."
                        )
            elif (
                msg.header.msgType == NinjaApiMessages_pb2.Header.WORKING_RULES_RESPONSE
            ):
                resp = NinjaApiWorkingRules_pb2.WorkingRules()
                resp.ParseFromString(msg.payload)
                for rule in resp.workingRules:
                    logging.info(
                        f"Found working rule '{rule.prefix}' "
                        f"with type {NinjaApiWorkingRules_pb2.WorkingRule.WorkType.Name(rule.workType)}"
                    )
            elif (
                msg.header.msgType
                == NinjaApiMessages_pb2.Header.PRICE_FEED_STATUS_RESPONSE
            ):
                resp = NinjaApiMarketData_pb2.PriceFeedStatus()
                resp.ParseFromString(msg.payload)
                logging.info(
                    f"Price feed status is {NinjaApiMarketData_pb2.PriceFeedStatus.Status.Name(resp.status)}"
                )
            elif (
                msg.header.msgType
                == NinjaApiMessages_pb2.Header.SECURITY_STATUSES_RESPONSE
            ):
                resp = NinjaApiMarketData_pb2.SecurityStatuses()
                resp.ParseFromString(msg.payload)
                for secStatus in resp.statuses:
                    logging.info(
                        f"Security status for {secStatus.contract.secDesc} is {NinjaApiMarketData_pb2.SecurityStatus.Status.Name(secStatus.status)}"
                    )
            time.sleep(0.01)

        self.disconnect()
