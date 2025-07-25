import logging
import time

import NinjaApiCommon_pb2
import NinjaApiContracts_pb2
import NinjaApiMessages_pb2
import NinjaApiPositions_pb2


from ninja_api_client import NinjaApiClient
from config import settings
import threading
from datetime import datetime, timedelta


class PositionsClient(NinjaApiClient):
    def __init__(self):
        super().__init__(settings.positions_host, settings.positions_port)
        cme = NinjaApiCommon_pb2.Exchange.CME
        self.products = {"NQU5": cme}
        self.accounts = ["FW077"]
        # DYNAMIC
        self.lastTime = datetime.now().replace(second=0, microsecond=80_000)
        self.positions = {}

    """ 
    Gets positions of specified contract(s) for account(s)
    Parameters:
        accounts (lst): List of the account identifiers 
        contractFilters (lst): List of the symbol(s) or product(s) name being traded (e.g., ['ESU5']).
        includeSpec (bool, optional): Whether to include speculative trades or not (mainly used for options). Default is False
    Returns:
        None, sends message for position request
    """

    def get_positions(self, accounts, contractFilters, includeSpec=False):
        getPositions = NinjaApiPositions_pb2.GetPositions()
        getPositions.accounts.extend(accounts)
        getPositions.filters.extend(contractFilters)
        getPositions.includeSpec = includeSpec

        container = NinjaApiMessages_pb2.MsgContainer()
        container.header.msgType = NinjaApiMessages_pb2.Header.POSITIONS_REQUEST
        container.payload = getPositions.SerializeToString()
        self.send_msg(container)

    """ 
    Gets positions of all contracts on all accounts
    Returns:
        None, sends multiple messages for position request
    """

    def get_all_positions(self):
        for account in self.accounts:
            for product in self.products.keys():
                contract = NinjaApiContracts_pb2.Contract()
                contract.exchange = self.products.get(product)
                contract.secDesc = product
                self.get_positions([account], [contract])

    def run(self):
        login = NinjaApiMessages_pb2.Login()
        login.user = settings.trading_user
        login.password = settings.trading_password
        login.connectionType = NinjaApiMessages_pb2.ConnectionType.POSITION_CONNECTION
        login.accessToken = settings.positions_access_token
        container = NinjaApiMessages_pb2.MsgContainer()
        container.header.msgType = NinjaApiMessages_pb2.Header.LOGIN_REQUEST
        container.header.version = "v1.0.0"
        container.payload = login.SerializeToString()
        self.send_msg(container)

        while self.connected:
            msg = self.recv_msg()
            if not msg:
                if (
                    datetime.now() > self.lastTime + timedelta(seconds=60)
                    and len(self.products) > 0
                    and len(self.accounts) > 0
                ):
                    self.get_all_positions()
                    self.lastTime = datetime.now().replace(second=0, microsecond=80_000)
                else:
                    continue
            elif msg.header.msgType == NinjaApiMessages_pb2.Header.LOGIN_RESPONSE:
                resp = NinjaApiMessages_pb2.LoginResponse()
                resp.ParseFromString(msg.payload)
                self.get_all_positions()
            elif msg.header.msgType == NinjaApiMessages_pb2.Header.POSITIONS_RESPONSE:
                resp = NinjaApiPositions_pb2.Positions()
                resp.ParseFromString(msg.payload)
                for position in resp.positions:
                    logging.info(
                        f"{position.account}, {position.contract.exchange}, {position.contract.secDesc}: {position.totalPos}"
                    )
                    self.positions[
                        (
                            position.account,
                            position.contract.exchange,
                            position.contract.secDesc,
                        )
                    ] = position.totalPos
            elif msg.header.msgType == NinjaApiMessages_pb2.Header.ERROR:
                error = NinjaApiMessages_pb2.Error()
                error.ParseFromString(msg.payload)
                logging.info(error.msg)

            time.sleep(0.01)

        self.disconnect()
