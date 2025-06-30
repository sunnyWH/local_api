import logging
import time

import NinjaApiCommon_pb2
import NinjaApiContracts_pb2
import NinjaApiMessages_pb2
import NinjaApiPositions_pb2

from ninja_api_client import NinjaApiClient
from config import settings


class PositionsClient(NinjaApiClient):
    def __init__(self):
        super().__init__(settings.positions_host, settings.positions_port)

    def getPositions(self, accounts, contractFilters, includeSpec=False):
        getPositions = NinjaApiPositions_pb2.GetPositions()
        getPositions.accounts.extend(accounts)
        getPositions.filters.extend(contractFilters)
        getPositions.includeSpec = includeSpec

        container = NinjaApiMessages_pb2.MsgContainer()
        container.header.msgType = NinjaApiMessages_pb2.Header.POSITIONS_REQUEST
        container.payload = getPositions.SerializeToString()
        self.send_msg(container)

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

        hb_count = 0
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
            elif msg.header.msgType == NinjaApiMessages_pb2.Header.LOGIN_RESPONSE:
                resp = NinjaApiMessages_pb2.LoginResponse()
                resp.ParseFromString(msg.payload)
                contract = NinjaApiContracts_pb2.Contract()
                contract.exchange = NinjaApiCommon_pb2.Exchange.CME
                contract.secDesc = "ZSN5"
                self.getPositions("ACCT1", contract)
            elif msg.header.msgType == NinjaApiMessages_pb2.Header.POSITIONS_RESPONSE:
                resp = NinjaApiPositions_pb2.Positions()
                resp.ParseFromString(msg.payload)
                for position in resp.positions:
                  logging.info(position)
            elif msg.header.msgType == NinjaApiMessages_pb2.Header.ERROR:
                error = NinjaApiMessages_pb2.Error()
                error.ParseFromString(msg.payload)
                logging.info(error.msg)
            time.sleep(0.01)

        self.disconnect()
