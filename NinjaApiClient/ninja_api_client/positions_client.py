import logging
import time

import NinjaApiCommon_pb2
import NinjaApiContracts_pb2
import NinjaApiMessages_pb2
import NinjaApiPositions_pb2
import NinjaApiOrderHandling_pb2


from ninja_api_client import NinjaApiClient
from config import settings
import threading
from datetime import datetime, timedelta


class PositionsClient(NinjaApiClient):
    def __init__(self):
        super().__init__(settings.positions_host, settings.positions_port)
        # DYNAMIC
        self.lastTime = datetime.now().replace(second=0, microsecond=80_000)
        self.contract = None

    def get_positions(self, accounts, contractFilters, includeSpec=False):
        getPositions = NinjaApiPositions_pb2.GetPositions()
        getPositions.accounts.extend(accounts)
        getPositions.filters.extend(contractFilters)
        getPositions.includeSpec = includeSpec

        container = NinjaApiMessages_pb2.MsgContainer()
        container.header.msgType = NinjaApiMessages_pb2.Header.POSITIONS_REQUEST
        container.payload = getPositions.SerializeToString()
        self.send_msg(container)

    def user_quits(self):
        while True:
            if input().strip().lower() == "q":
                logging.info("POSITIONS_CLIENT: Disconnecting...")
                self.disconnect()

    def check_pos(self):
        while True:
            if (
                datetime.now() > self.lastTime + timedelta(seconds=60)
                and self.contract is not None
            ):
                self.get_positions(["FW077"], [self.contract])
                self.lastTime = datetime.now().replace(second=0, microsecond=80_000)

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
        ##________________________________________________________________________________
        ## USER INPUT
        threading.Thread(target=self.user_quits, daemon=True).start()
        ##________________________________________________________________________________
        ## TRADE LOGIC
        threading.Thread(target=self.check_pos, daemon=True).start()

        while self.connected:
            msg = self.recv_msg()
            if not msg:
                continue
            if msg.header.msgType == NinjaApiMessages_pb2.Header.LOGIN_RESPONSE:
                resp = NinjaApiMessages_pb2.LoginResponse()
                resp.ParseFromString(msg.payload)
                contract = NinjaApiContracts_pb2.Contract()
                contract.exchange = NinjaApiCommon_pb2.Exchange.CME
                contract.secDesc = "NQU5"
                self.contract = contract
                self.get_positions(["FW077"], [self.contract])
            elif msg.header.msgType == NinjaApiMessages_pb2.Header.POSITIONS_RESPONSE:
                resp = NinjaApiPositions_pb2.Positions()
                resp.ParseFromString(msg.payload)
                for position in resp.positions:
                    logging.info(f"{position.contract.secDesc}: {position.totalPos}")
            elif msg.header.msgType == NinjaApiMessages_pb2.Header.ERROR:
                error = NinjaApiMessages_pb2.Error()
                error.ParseFromString(msg.payload)
                logging.info(error.msg)
            time.sleep(0.01)

        self.disconnect()
