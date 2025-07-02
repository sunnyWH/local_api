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

# USER IMPORTS
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from datetime import datetime, timedelta
from datetime import time as datetimeTime
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
        self.toPrint = True
        self.marketStart = datetimeTime(8, 30)
        self.marketEnd = datetimeTime(14, 59)
        self.printed = False

        # DYNAMIC PARAMETERS
        self.signal = 0
        self.votes = deque(maxlen=self.voteCount)
        self.finalVotes = deque(maxlen=self.finalVotesCount)
        self.votesFull = False
        self.voteTotal = 0
        self.lastPrice = None
        self.lastTime = None
        self.currentTime = None
        self.inMarket = None

    def user_quits(self):
        while True:
            if input("Type 'q' to quit: ").strip().lower() == "q":
                logging.info("Quit signal received.")
                container = NinjaApiMessages_pb2.MsgContainer()
                stopmd = NinjaApiMarketData_pb2.StopMarketData()
                container.header.msgType = (
                    NinjaApiMessages_pb2.Header.STOP_MARKET_DATA_REQUEST
                )
                logging.info("Stopping market connection...")
                container.payload = stopmd.SerializeToString()
                self.send_msg(container)
                logging.info("Disconnecting...")
                self.disconnect()

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
        df["sending_time"] = pd.to_datetime(df["sending_time"])
        df.index = df["sending_time"]
        df = df.sort_index()
        times = df.index.time
        mask = (times >= pd.to_datetime("08:30").time()) & (
            times < pd.to_datetime("17:00").time()
        )
        df = df[mask].copy()
        df["minute"] = df["sending_time"].dt.floor("min")
        # mask = df["sending_time"] > df["minute"]
        # df = df[mask]
        df = df.loc[df.groupby("minute")["sending_time"].idxmin()]  # last traded price
        df = df.drop(columns=["sending_time", "minute"])
        df["t_price"] = df["t_price"] / self.productDiv
        moves = df["t_price"].diff()
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
                pass

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
            self.signal = 0
            if toPrint:
                logging.info(
                    f"FinalVotes: {list(self.finalVotes)}, Signal: {self.signal}"
                )

    def run(self):
        ##________________________________________________________________________________
        ## WARMUP
        self.warmup()
        ##________________________________________________________________________________
        ## USER INPUT
        threading.Thread(target=self.user_quits, daemon=True).start()
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
        while self.connected:  ## ONCE MARKET DATA IS DISCONNECTED, STOP THE CONNECTION
            self.currentTime = datetime.now().time()
            if (
                self.marketStart <= self.currentTime <= self.marketEnd
                and not self.printed
            ):
                self.printed = True
                self.inMarket = True
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
                        latestTradePrice = latestTrade.tradePrice / self.productDiv
                        # logging.info(
                        #     f"{latestTrade.transactTime.timestamp}: {latestTradePrice}, {latestTrade.tradeQty}"
                        # )
                        if datetime.combine(
                            datetime.today().date(), self.currentTime
                        ) > datetime.combine(
                            datetime.today().date(), self.lastTime
                        ) + timedelta(
                            seconds=60
                        ):
                            logging.info(f"Contract: {update.contract.secDesc}")
                            logging.info(
                                f"Bid Qty: {update.tobUpdate.bidQty} | Bid: {update.tobUpdate.bidPrice} | Ask: {update.tobUpdate.askPrice} | Ask Qty: {update.tobUpdate.askQty}"
                            )
                            if self.lastPrice is None:
                                self.lastPrice = latestTradePrice
                                logging.info(
                                    f"Established latestPrice: {latestTradePrice}"
                                )
                            else:
                                move = latestTradePrice - self.lastPrice
                                self.add_votes(move, toPrint=self.toPrint)
                                self.check_votes_for_final_votes(toPrint=self.toPrint)
                                self.check_signal_from_final_votes(toPrint=self.toPrint)
                                self.lastPrice = latestTradePrice
                            logging.info(f"Latest Price: {latestTradePrice}")
                            self.lastTime = self.currentTime.replace(
                                second=0, microsecond=0
                            )
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
