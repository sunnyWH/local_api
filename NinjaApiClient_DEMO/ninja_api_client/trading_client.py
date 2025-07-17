import logging
import time

import NinjaApiCommon_pb2
import NinjaApiContracts_pb2
import NinjaApiMarketData_pb2
import NinjaApiMessages_pb2
import NinjaApiOrderHandling_pb2
import NinjaApiSheets_pb2
import NinjaApiWorkingRules_pb2

from ninja_api_client import NinjaApiClient
from config import settings


class TradingClient(NinjaApiClient):
    def __init__(self):
        super().__init__(settings.trading_host, settings.trading_port)

    def run(self):
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
        startmd = NinjaApiMarketData_pb2.StartMarketData()
        contract = startmd.contracts.add()
        contract.exchange = NinjaApiCommon_pb2.Exchange.CME
        contract.secDesc = "ZSX5"
        contract.whName = "ZSX5"
        startmd.cadence.duration = 1000  # 1s
        startmd.includeImplieds = True
        startmd.includeTradeUpdates = True
        container.header.msgType = NinjaApiMessages_pb2.Header.START_MARKET_DATA_REQUEST
        container.payload = startmd.SerializeToString()
        self.send_msg(container)

        hb_count = 0
        md_update_count = 0
        while self.connected:
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
                    if (
                        md_update_count == 0 and update.contract.secDesc == "ZSX5"
                    ):  # Send order add after first ZSX5 market update is seen
                        orderadd = NinjaApiOrderHandling_pb2.OrderAdd()
                        orderadd.account = "ACCT1"
                        orderadd.contract.exchange = NinjaApiCommon_pb2.Exchange.CME
                        orderadd.contract.secDesc = contract.secDesc
                        orderadd.contract.whName = contract.whName
                        orderadd.timeInForce.type = (
                            NinjaApiOrderHandling_pb2.TimeInForce.Type.GTC
                        )
                        orderadd.side = NinjaApiCommon_pb2.Side.BUY
                        orderadd.qty = 2
                        orderadd.price = (
                            update.tobUpdate.bidPrice - 2
                        )  # bid price plus some edge
                        container.header.msgType = (
                            NinjaApiMessages_pb2.Header.ORDER_ADD_REQUEST
                        )
                        container.payload = orderadd.SerializeToString()
                        self.send_msg(container)
                md_update_count += 1
                if md_update_count == 5:
                    # Stop receiving market updates after the first 10 updates have been received
                    stopmd = NinjaApiMarketData_pb2.StopMarketData()
                    container.header.msgType = (
                        NinjaApiMessages_pb2.Header.STOP_MARKET_DATA_REQUEST
                    )
                    container.payload = stopmd.SerializeToString()
                    self.send_msg(container)
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
                    f"Error code: {NinjaApiOrderHandling_pb2.OrderErrorCode.Name(resp.errorCode)}. "
                    f'Reason: ("{resp.reason}").'
                )
            elif msg.header.msgType == NinjaApiMessages_pb2.Header.ORDER_ADD_EVENT:
                resp = NinjaApiOrderHandling_pb2.OrderAddEvent()
                resp.ParseFromString(msg.payload)
                logging.info(
                    f"Received order add event for {resp.contract.secDesc} "
                    f"with order number ({resp.orderNo}). "
                    f"Price: {resp.price}, Side: {NinjaApiCommon_pb2.Side.Name(resp.side)}, Qty: {resp.qty}"
                )
                # Get all active API orders
                getactiveorders = NinjaApiOrderHandling_pb2.GetActiveOrders()
                getactiveorders.showOnlyApiOrders = True
                container.header.msgType = (
                    NinjaApiMessages_pb2.Header.ACTIVE_ORDERS_REQUEST
                )
                container.payload = getactiveorders.SerializeToString()
                self.send_msg(container)
                # Send an order change upping quantity and giving an extra tick of edge
                orderchange = NinjaApiOrderHandling_pb2.OrderChange()
                orderchange.orderNo = resp.orderNo
                orderchange.qty = resp.qty + 1
                orderchange.price = resp.price - 1
                container.header.msgType = (
                    NinjaApiMessages_pb2.Header.ORDER_CHANGE_REQUEST
                )
                container.payload = orderchange.SerializeToString()
                self.send_msg(container)
            elif msg.header.msgType == NinjaApiMessages_pb2.Header.ORDER_CHANGE_FAILURE:
                resp = NinjaApiOrderHandling_pb2.OrderChangeFailure()
                resp.ParseFromString(msg.payload)
                logging.info(
                    f"Received order change failure for {resp.contract.secDesc} "
                    f"with order number ({resp.orderNo}). "
                    f"Error code: {NinjaApiOrderHandling_pb2.OrderErrorCode.Name(resp.errorCode)}. "
                    f'Reason: ("{resp.reason}").'
                )
            elif msg.header.msgType == NinjaApiMessages_pb2.Header.ORDER_CHANGE_EVENT:
                resp = NinjaApiOrderHandling_pb2.OrderChangeEvent()
                resp.ParseFromString(msg.payload)
                logging.info(
                    f"Received order change event for {resp.contract.secDesc} "
                    f"with order number ({resp.orderNo}). "
                    f"Price: {resp.price}, Side: {NinjaApiCommon_pb2.Side.Name(resp.side)}, Qty: {resp.qty}"
                )
                ordercancel = NinjaApiOrderHandling_pb2.OrderCancel()
                ordercancel.orderNo = resp.orderNo
                container.header.msgType = (
                    NinjaApiMessages_pb2.Header.ORDER_CANCEL_REQUEST
                )
                container.payload = ordercancel.SerializeToString()
                self.send_msg(container)
            elif msg.header.msgType == NinjaApiMessages_pb2.Header.ORDER_CANCEL_FAILURE:
                resp = NinjaApiOrderHandling_pb2.OrderCancelFailure()
                resp.ParseFromString(msg.payload)
                logging.info(
                    f"Received order cancel failure for order {resp.orderNo} on contract {resp.contract.secDesc}"
                )
            elif msg.header.msgType == NinjaApiMessages_pb2.Header.ORDER_CANCEL_EVENT:
                resp = NinjaApiOrderHandling_pb2.OrderCancelEvent()
                resp.ParseFromString(msg.payload)
                logging.info(
                    f"Canceled order {resp.orderNo} on contract {resp.contract.secDesc}"
                )
            elif msg.header.msgType == NinjaApiMessages_pb2.Header.MASS_CANCEL_EVENT:
                resp = NinjaApiOrderHandling_pb2.MassCancelEvent()
                resp.ParseFromString(msg.payload)
                logging.info(
                    f"{len(resp.canceledOrders)} were canceled. They are: "
                )
                for order in resp.canceledOrders:
                    logging.info(f"  Canceled order {order.orderNo} on contract {order.contract.secDesc}")
            elif msg.header.msgType == NinjaApiMessages_pb2.Header.ORDER_REJECT_EVENT:
                resp = NinjaApiOrderHandling_pb2.OrderRejectEvent()
                resp.ParseFromString(msg.payload)
                logging.info(
                    f"Received order reject event for {resp.contract.secDesc} "
                    f"with order number ({resp.orderNo}). "
                    f'Reason: ("{resp.reason}").'
                )
            elif msg.header.msgType == NinjaApiMessages_pb2.Header.FILL_NOTICE:
                resp = NinjaApiOrderHandling_pb2.FillNotice()
                resp.ParseFromString(msg.payload)
                logging.info(
                    f"Filled on {resp.qty if resp.side == NinjaApiCommon_pb2.Side.BUY else -resp.qty} "
                    f"{resp.contract.secDesc} ({resp.orderNo}) at a price of {resp.price}. "
                )
                if resp.legFills:
                    for legFill in resp.legFills:
                        logging.info(
                            f"Filled {legFill.qty if legFill.side == NinjaApiCommon_pb2.Side.BUY else -legFill.qty} "
                            f"of leg {legFill.contract.secDesc} at {legFill.price}."
                        )
            elif msg.header.msgType == NinjaApiMessages_pb2.Header.ACTIVE_ORDERS_RESPONSE:
                resp = NinjaApiOrderHandling_pb2.ActiveOrders()
                resp.ParseFromString(msg.payload)
                for activeorder in resp.activeOrders:
                    logging.info(
                        f"Found active API-managed order {activeorder.orderNo} "
                        f"for contract {activeorder.contract.secDesc} "
                        f"with qty {activeorder.qty} and price {activeorder.price}"
                    )

            time.sleep(0.01)

        self.disconnect()
