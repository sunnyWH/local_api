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
from collections import deque
import pandas as pd
import numpy as np


class TradingClient(NinjaApiClient):

    def __init__(self):
        super().__init__(settings.trading_host, settings.trading_port)
        # CONSTANT PARAMETERS
        self.product = "NQU5"
        self.product_div = 100
        self.vote_div = 2
        self.vote_count = 3
        self.lookback = 250
        # DYNAMIC PARAMETERS
        self.votes = deque(maxlen=self.lookback)
        self.vote_total = 0
        self.last_price = None

    def warmup(self):
        engine = create_engine(
            "postgresql+psycopg2://tickreader:tickreader@tsdb:5432/cme"
        )
        query = f"""
            SELECT wh_name, t_price, sending_time
            FROM nq_fut_trades_weekly
            WHERE wh_name = %s
            ORDER BY sending_time DESC
            LIMIT 500_000 
        """
        df = pd.read_sql(query, engine, params=(self.product,))
        df.drop(columns=["wh_name"], inplace=True)
        df["sending_time"] = pd.to_datetime(df["sending_time"])
        df.index = df["sending_time"]
        df = df.sort_index()
        time_diffs = df.index.to_series().diff()
        gap_mask = time_diffs > pd.Timedelta(minutes=55)  # don't use overnight
        last_gap_index = gap_mask[gap_mask].index[-1] if gap_mask.any() else None
        df = df[df.index < pd.to_datetime(last_gap_index)].copy()
        df["minute"] = df["sending_time"].dt.floor("min")
        mask = df["sending_time"] > df["minute"]
        df = df[mask]
        df = df.loc[df.groupby("minute")["sending_time"].idxmax()]  # last traded price
        df = df.drop(columns=["sending_time", "minute"])
        moves = df["t_price"].diff() / self.product_div
        for move in moves:
            self.add_votes(move, to_print=True)

    def add_votes(self, move, to_print=False):
        if not isinstance(move, (int, float)):
            logging.info(f"Move given is not valid: {move}")
            return
        if np.isnan(move):
            logging.info(f"Move given is NA: {move}")
            return
        to_add = int(move / self.vote_div)
        if to_add > 0:
            for i in np.arange(to_add):
                self.votes.append(1)
        elif to_add < 0:
            for i in np.arange(abs(to_add)):
                self.votes.append(-1)
        self.vote_total = sum(self.votes)
        if to_print:
            logging.info(
                f"Change: {move}, Votes: {to_add}, Total Vote: {self.vote_total}"
            )

    def run(self):
        ##________________________________________________________________________________
        ## WARMUP
        self.warmup()
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
        startmd.cadence.duration = 1000  # 1s
        startmd.includeImplieds = True
        startmd.includeTradeUpdates = True
        container.header.msgType = NinjaApiMessages_pb2.Header.START_MARKET_DATA_REQUEST
        container.payload = startmd.SerializeToString()
        self.send_msg(container)

        hb_count = 0
        md_update_count = 0
        while self.connected:  ## ONCE MARKET DATA IS DISCONNECTED, STOP THE CONNECTION
            msg = self.recv_msg()
            if not msg:
                continue
            logging.info(
                "Received "
                + NinjaApiMessages_pb2.Header.MsgType.Name(msg.header.msgType)
            )
            if msg.header.msgType == NinjaApiMessages_pb2.Header.HEARTBEAT:
                hb_count += 1
                if hb_count == 3:
                    break  # Exit loop after receiving 3 heartbeats
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
            ##________________________________________________________________________________
            ## ON EVERY MARKET UPDATE
            elif msg.header.msgType == NinjaApiMessages_pb2.Header.MARKET_UPDATES:
                resp = NinjaApiMarketData_pb2.MarketUpdates()
                resp.ParseFromString(msg.payload)
                for update in resp.marketUpdates:
                    logging.info(f"Contract: {update.contract.secDesc}")
                    logging.info(
                        f"Bid Qty: {update.tobUpdate.bidQty} | Bid: {update.tobUpdate.bidPrice} | Ask: {update.tobUpdate.askPrice} | Ask Qty: {update.tobUpdate.askQty}"
                    )
                    logging.info(
                        f"{len(update.tradeUpdates)} trades have occurred since the last market update"
                    )
                md_update_count += 1
                if md_update_count == 6:
                    # Stop receiving market updates after the first 5 updates have been received
                    stopmd = NinjaApiMarketData_pb2.StopMarketData()
                    container.header.msgType = (
                        NinjaApiMessages_pb2.Header.STOP_MARKET_DATA_REQUEST
                    )
                    container.payload = stopmd.SerializeToString()
                    self.send_msg(container)
            time.sleep(0.01)

        self.disconnect()
