from ninja_api_client import NinjaApiClient
from config import settings

import NinjaApiCommon_pb2
import NinjaApiContracts_pb2
import NinjaApiMarketData_pb2
import NinjaApiMessages_pb2
import NinjaApiOrderHandling_pb2
import NinjaApiSheets_pb2
import NinjaApiWorkingRules_pb2

from datetime import datetime, timedelta
import logging
import time
import threading
from HELPERS import TradingLogger


class TradingClient(NinjaApiClient):

    def __init__(self):
        super().__init__(settings.trading_host, settings.trading_port)
        self.logging = True
        self.logger = TradingLogger()
        cme = NinjaApiCommon_pb2.Exchange.CME
        self.lastOrderCheck = datetime.now()
        self.lastPrintTime = datetime.now().replace(second=0)
        self.products = {"NQU5": cme}
        self.accounts = ["FW077", "FW078", "FW079", "FW080"]
        self.latest_trade_price = {product: None for product in self.products.keys()}
        self.latest_bid = {product: None for product in self.products.keys()}
        self.latest_ask = {product: None for product in self.products.keys()}
        self.latest_low = {product: None for product in self.products.keys()}
        self.latest_high = {product: None for product in self.products.keys()}
        self.latest_volume = {product: 0 for product in self.products.keys()}
        self.inOrderChange = {}
        self.activeOrders = {}
        self.activeOrderCounter = 0
        self.fillCounter = 0

    """
    Get latest bid
    Parameters:
        product (str): The symbol or product name being traded (e.g., 'ESU5').
    """

    def get_bid(self, product):
        attempts = 0
        max_attempts = 1_000
        while attempts < max_attempts:
            bid = self.latest_bid.get(product)
            if bid is not None:
                return bid
            attempts += 1
        return None

    """
    Get latest ask
    Parameters:
        product (str): The symbol or product name being traded (e.g., 'ESU5').
    """

    def get_ask(self, product):
        attempts = 0
        max_attempts = 1_000
        while attempts < max_attempts:
            ask = self.latest_ask.get(product)
            if ask is not None:
                return ask
            attempts += 1
        return None

    """
    Get latest high
    Parameters:
        product (str): The symbol or product name being traded (e.g., 'ESU5').
    """

    def get_high(self, product):
        attempts = 0
        max_attempts = 1_000
        while attempts < max_attempts:
            high = self.latest_high.get(product)
            if high is not None:
                return high
            attempts += 1
        return None

    """
    Get latest low
    Parameters:
        product (str): The symbol or product name being traded (e.g., 'ESU5').
    """

    def get_low(self, product):
        attempts = 0
        max_attempts = 1_000
        while attempts < max_attempts:
            low = self.latest_low.get(product)
            if low is not None:
                return low
            attempts += 1
        return None

        """
    Get latest traded price
    Parameters:
        product (str): The symbol or product name being traded (e.g., 'ESU5').
    """

    def get_trade_price(self, product):
        while True:
            trade_price = self.latest_trade_price.get(product)
            if trade_price is not None:
                return trade_price

    """
    Get volume of this trade update (since turninig on the connection, warmup for day will have to be done on algo side)
    Parameters:
        product (str): The symbol or product name being traded (e.g., 'ESU5').
    """

    def get_volume(self, product):
        while True:
            volume = self.latest_volume.get(product)
            if volume != 0:
                return volume

    """
    Submit an order to the specified exchange.
    Parameters:
        account (str): The account identifier to place the order from.
        product (str): The symbol or product name being traded (e.g., 'ESU5').
        price (float): The price at which to place the order.
        qty (int): The number of contracts/shares to trade.
        worker (str, optional): Identifier for the worker or system submitting the order. Default is "w".
        exchange (Exchange Enum, optional): The exchange to route the order to (e.g., CME). Default is NinjaApiCommon_pb2.Exchange.CME.
        tag (str, optional): A custom string for tagging or identifying the order. Default is "".
        log (bool, optional): Whether to log the order submission. Default is True.
    Returns:
        None
    """

    def order(
        self,
        account,
        product,
        price,
        qty,
        worker="w",
        exchange=NinjaApiCommon_pb2.Exchange.CME,
        tag="",
        # log=True,
    ):
        container = NinjaApiMessages_pb2.MsgContainer()
        orderadd = NinjaApiOrderHandling_pb2.OrderAdd()
        orderadd.account = account
        orderadd.contract.exchange = exchange
        orderadd.contract.secDesc = product
        orderadd.contract.whName = product
        orderadd.timeInForce.type = NinjaApiOrderHandling_pb2.TimeInForce.Type.GTC
        if qty < 0:
            orderadd.side = NinjaApiCommon_pb2.Side.SELL
        else:
            orderadd.side = NinjaApiCommon_pb2.Side.BUY
        orderadd.qty = abs(qty)
        orderadd.price = price
        orderadd.prefix = worker
        container.header.msgType = NinjaApiMessages_pb2.Header.ORDER_ADD_REQUEST
        container.payload = orderadd.SerializeToString()
        self.send_msg(container)

    """
    Cancels ALL working orders and submits market order to flatten at the specifed price. 
    Parameters:
        account (str): The account identifier to place the order from.
        product (str): The symbol or product name being traded (e.g., 'ESU5').
        price (float): The price at which to place the order.
        qty (int): The number of contracts/shares to trade.
        worker (str, optional): Identifier for the worker or system submitting the order. Default is "w".
        exchange (Exchange Enum, optional): The exchange to route the order to (e.g., CME). Default is NinjaApiCommon_pb2.Exchange.CME.
        tag (str, optional): A custom string for tagging or identifying the order. Default is "".
        log (bool, optional): Whether to log the order submission. Default is True.
    Returns:
        None
    """

    def flatten(
        self,
        account,
        product,
        price,
        qty,
        worker="w",
        exchange=NinjaApiCommon_pb2.Exchange.CME,
        tag="",
    ):
        container = NinjaApiMessages_pb2.MsgContainer()
        ordercancel = NinjaApiOrderHandling_pb2.CancelAllOrders()
        ordercancel.cancelGTCs = True
        container.header.msgType = NinjaApiMessages_pb2.Header.CANCEL_ALL_ORDERS_REQUEST
        container.payload = ordercancel.SerializeToString()
        self.send_msg(container)
        self.order(account, product, price, qty, tag=tag)

    """
    Change order according to given parameters
    Parameters:
        orderNo (str): The order number/identifier that we want to change
        price (float): The price at which to change the order.
        qty (int): The number of contracts/shares to trade.
        
    Returns:
        None
    """
    # # CANCELS OLD ORDER AND SENDS NEW ORDER IN
    # def change_order(
    #     self, orderNo, price, qty, worker="w", account="", product="", exchange=1
    # ):
    #     # change order to new price and qty
    #     if qty < 0:
    #         logging.info(
    #             "Negative qtys are transitioned to positive as side captures direction."
    #         )
    #         qty = abs(qty)
    #     # cancel existing order
    #     self.cancel_order(orderNo)
    #     # put in new order
    #     self.order(account, product, price, qty, worker, exchange)

    # JUST CHANGES THE ORDER
    def change_order(
        self, orderNo, price, qty, worker="w", account="", product="", exchange=1
    ):
        # change order to new price and qty
        if qty < 0:
            logging.info(
                "Negative qtys are transitioned to positive as side captures direction."
            )
            qty = abs(qty)
        container = NinjaApiMessages_pb2.MsgContainer()
        orderchange = NinjaApiOrderHandling_pb2.OrderChange()
        orderchange.orderNo = orderNo
        orderchange.qty = qty
        orderchange.price = price
        orderchange.prefix = worker
        container.header.msgType = NinjaApiMessages_pb2.Header.ORDER_CHANGE_REQUEST
        container.payload = orderchange.SerializeToString()
        self.send_msg(container)

    # # CHANGES ORDER TO G WORKER THEN ASSIGNS WORKER TO IT
    # def change_order(
    #     self, orderNo, price, qty, worker="w", account="", product="", exchange=1
    # ):
    #     # change order to new price and qty
    #     if qty < 0:
    #         logging.info(
    #             "Negative qtys are transitioned to positive as side captures direction."
    #         )
    #         qty = abs(qty)
    #     container = NinjaApiMessages_pb2.MsgContainer()
    #     orderchange = NinjaApiOrderHandling_pb2.OrderChange()
    #     orderchange.orderNo = orderNo
    #     orderchange.qty = qty
    #     orderchange.price = price
    #     orderchange.prefix = "G"
    #     container.header.msgType = NinjaApiMessages_pb2.Header.ORDER_CHANGE_REQUEST
    #     container.payload = orderchange.SerializeToString()
    #     self.send_msg(container)
    #     while self.activeOrders.get(orderNo).prefix != "G":
    #         time.sleep(0.01)
    #     container = NinjaApiMessages_pb2.MsgContainer()
    #     orderchange = NinjaApiOrderHandling_pb2.OrderChange()
    #     orderchange.orderNo = orderNo
    #     orderchange.prefix = worker
    #     container.header.msgType = NinjaApiMessages_pb2.Header.ORDER_CHANGE_REQUEST
    #     container.payload = orderchange.SerializeToString()

    """
    Cancel order according to orderNo
    Parameters:
        orderNo (str): The order number/identifier that we want to change
    Returns:
        None
    """

    def cancel_order(self, orderNo):
        container = NinjaApiMessages_pb2.MsgContainer()
        ordercancel = NinjaApiOrderHandling_pb2.OrderCancel()
        ordercancel.orderNo = orderNo
        container.header.msgType = NinjaApiMessages_pb2.Header.ORDER_CANCEL_REQUEST
        container.payload = ordercancel.SerializeToString()
        self.send_msg(container)

    """
    Mass Cancel all orders
    Parameters:
        None
    Returns:
        None
    """

    def mass_cancel(self):
        container = NinjaApiMessages_pb2.MsgContainer()
        masscancel = NinjaApiOrderHandling_pb2.CancelAllOrders()
        container.header.msgType = NinjaApiMessages_pb2.Header.CANCEL_ALL_ORDERS_REQUEST
        container.payload = masscancel.SerializeToString()
        self.send_msg(container)
        logging.info(f"Mass Cancel Request Sent")

    """
    Check order according to orderNo
    Parameters:
        orderNo (str): The order number/identifier that we want to change
    Returns:
        None
    """

    def check_orders(self):
        container = NinjaApiMessages_pb2.MsgContainer()
        getactiveorders = NinjaApiOrderHandling_pb2.GetActiveOrders()
        getactiveorders.showOnlyApiOrders = True
        container.header.msgType = NinjaApiMessages_pb2.Header.ACTIVE_ORDERS_REQUEST
        container.payload = getactiveorders.SerializeToString()
        self.send_msg(container)

    def run(self):
        # region LOGIN
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
        # region REQUEST AVAILABLE FIELDS
        container.header.msgType = NinjaApiMessages_pb2.Header.NINJA_REQUEST
        container.payload = b""
        self.send_msg(container)
        container.header.msgType = NinjaApiMessages_pb2.Header.ACCOUNTS_REQUEST
        self.send_msg(container)
        container.header.msgType = NinjaApiMessages_pb2.Header.WORKING_RULES_REQUEST
        self.send_msg(container)
        container.header.msgType = NinjaApiMessages_pb2.Header.PRICE_FEED_STATUS_REQUEST
        self.send_msg(container)
        sheets = NinjaApiSheets_pb2.Sheets()
        container.header.msgType = NinjaApiMessages_pb2.Header.SHEETS_REQUEST
        container.payload = sheets.SerializeToString()
        self.send_msg(container)

        ##________________________________________________________________________________
        # region START MARKET DATA
        startmd = NinjaApiMarketData_pb2.StartMarketData()
        for product in self.products.keys():
            contract = startmd.contracts.add()
            contract.exchange = self.products.get(product)
            contract.secDesc = product
            contract.whName = product
        startmd.cadence.duration = 0  # in milliseconds
        startmd.includeImplieds = True
        startmd.includeTradeUpdates = True
        container.header.msgType = NinjaApiMessages_pb2.Header.START_MARKET_DATA_REQUEST
        container.payload = startmd.SerializeToString()
        self.send_msg(container)
        while self.connected:
            if datetime.now() > self.lastPrintTime + timedelta(seconds=60):
                if len(self.activeOrders) > 0:
                    logging.info(
                        "| ".join(
                            f"{order.orderNo}: {order.account}, {order.price}, {order.qty*((-2*order.side)+3)}"
                            for order in self.activeOrders.values()
                        )
                    )
                else:
                    logging.info("No Active Orders")
                self.lastPrintTime = datetime.now().replace(second=0)
            if datetime.now() > self.lastOrderCheck + timedelta(microseconds=50_000):
                self.check_orders()
                self.lastOrderCheck = datetime.now()

            msg = self.recv_msg()
            if not msg:
                continue

            # ________________________________________________________________________________
            # region ON EVERY MARKET UPDATE
            elif msg.header.msgType == NinjaApiMessages_pb2.Header.MARKET_UPDATES:
                resp = NinjaApiMarketData_pb2.MarketUpdates()
                resp.ParseFromString(msg.payload)
                for update in resp.marketUpdates:
                    product = update.contract.secDesc
                    if len(update.tradeUpdates) > 0:
                        high = None
                        low = None
                        volume = 0
                        if len(update.tradeUpdates) == 1:
                            high = update.tradeUpdates[0].tradePrice
                            low = update.tradeUpdates[0].tradePrice
                            volume = update.tradeUpdates[0].tradeQty
                        else:
                            for trade in update.tradeUpdates:
                                if high is None:
                                    high = trade.tradePrice
                                elif trade.tradePrice > high:
                                    high = trade.tradePrice
                                if low is None:
                                    low = trade.tradePrice
                                elif trade.tradePrice < low:
                                    low = trade.tradePrice
                                volume += trade.tradeQty

                        self.latest_trade_price[product] = update.tradeUpdates[
                            -1
                        ].tradePrice
                        self.latest_high[product] = high
                        self.latest_low[product] = low
                        self.latest_bid[product] = update.tobUpdate.bidPrice
                        self.latest_ask[product] = update.tobUpdate.askPrice
                        self.latest_volume[product] += volume

                # logging.info(f"trade_price: {self.latest_trade_price}")
                # logging.info(f"bid: {self.latest_bid}")
                # logging.info(f"ask: {self.latest_ask}")
                # logging.info(f"low: {self.latest_low}")
                # logging.info(f"high: {self.latest_high}")
            ##________________________________________________________________________________
            # region PRINT AVAILABLE FIELDS
            elif (
                msg.header.msgType == NinjaApiMessages_pb2.Header.ACTIVE_ORDERS_RESPONSE
            ):
                resp = NinjaApiOrderHandling_pb2.ActiveOrders()
                resp.ParseFromString(msg.payload)
                self.activeOrders.clear()
                for activeorder in resp.activeOrders:
                    if activeorder.qty != 0:
                        self.activeOrders[activeorder.orderNo] = activeorder
                self.activeOrderCounter += 1
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
            elif msg.header.msgType == NinjaApiMessages_pb2.Header.ORDER_ADD_FAILURE:
                resp = NinjaApiOrderHandling_pb2.OrderAddFailure()
                resp.ParseFromString(msg.payload)
                logging.info(
                    f"Received order add failure for {resp.contract.secDesc} "
                    f"with order number ({resp.orderNo}). "
                    f"Error code: {resp.errorCode}. "
                    f'Reason: ("{resp.reason}").'
                )
            elif msg.header.msgType == NinjaApiMessages_pb2.Header.ORDER_CANCEL_EVENT:
                resp = NinjaApiOrderHandling_pb2.OrderCancelEvent()
                resp.ParseFromString(msg.payload)
                logging.info(
                    f"Canceled order {resp.orderNo} on contract {resp.contract.secDesc}"
                )
            elif msg.header.msgType == NinjaApiMessages_pb2.Header.ORDER_CANCEL_FAILURE:
                resp = NinjaApiOrderHandling_pb2.OrderCancelFailure()
                resp.ParseFromString(msg.payload)
                logging.info(
                    f"Received order cancel failure for order {resp.orderNo} on contract {resp.contract.secDesc}"
                )
            elif msg.header.msgType == NinjaApiMessages_pb2.Header.ORDER_CHANGE_EVENT:
                resp = NinjaApiOrderHandling_pb2.OrderChangeEvent()
                resp.ParseFromString(msg.payload)
                logging.info(
                    f"Received order change event for {resp.contract.secDesc} "
                    f"with order number ({resp.orderNo}). "
                    f"Price: {resp.price}, Side: {NinjaApiCommon_pb2.Side.Name(resp.side)}, Qty: {resp.qty}"
                )
            elif msg.header.msgType == NinjaApiMessages_pb2.Header.ORDER_CHANGE_FAILURE:
                resp = NinjaApiOrderHandling_pb2.OrderChangeFailure()
                resp.ParseFromString(msg.payload)
                logging.info(
                    f"Received order change failure for {resp.contract.secDesc} "
                    f"with order number ({resp.orderNo}), side ({resp.side}), qty ({resp.qty}), price ({resp.price}), worker ({resp.prefix}). "
                    f"Error code: {NinjaApiMessages_pb2.Error.Type.Name(resp.errorCode)}."
                    f'Reason: ("{resp.reason}").'
                )
            elif msg.header.msgType == NinjaApiMessages_pb2.Header.MASS_CANCEL_EVENT:
                resp = NinjaApiOrderHandling_pb2.MassCancelEvent()
                resp.ParseFromString(msg.payload)
                logging.info(f"{len(resp.canceledOrders)} were canceled. They are: ")
                for order in resp.canceledOrders:
                    logging.info(
                        f"Canceled order {order.orderNo} on contract {order.contract.secDesc}"
                    )
            elif msg.header.msgType == NinjaApiMessages_pb2.Header.FILL_NOTICE:
                resp = NinjaApiOrderHandling_pb2.FillNotice()
                resp.ParseFromString(msg.payload)
                self.fillCounter += 1
                logging.info(
                    f"Filled on {resp.qty if resp.side == NinjaApiCommon_pb2.Side.BUY else -resp.qty} "
                    f"{resp.contract.secDesc} ({resp.orderNo}) at a price of {resp.price}. "
                )
                if self.logging:
                    # log the trade in txt
                    tradeTime = datetime.fromtimestamp(
                        resp.transactTime.timestamp / 1e9
                    )
                    if resp.side == NinjaApiCommon_pb2.Side.BUY:
                        self.logger.log_trade(
                            tradeTime,
                            resp.price,
                            resp.qty,
                            resp.account,
                        )
                    elif resp.side == NinjaApiCommon_pb2.Side.SELL:
                        self.logger.log_trade(
                            tradeTime,
                            resp.price,
                            -1 * resp.qty,
                            resp.account,
                        )

            time.sleep(0.02)

        self.disconnect()
