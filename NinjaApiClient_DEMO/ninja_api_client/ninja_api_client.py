import logging
import socket
import struct
import threading
import time

from abc import ABC, abstractmethod
from typing import Optional

import NinjaApiMessages_pb2


class NinjaApiClient(ABC):
    def __init__(self, host: str, port: int):
        self.connected = False
        self.host = host
        self.port = port
        self.frame = b""
        self.msg = b""
        self.send_lock = threading.Lock()  # Guards self.sock.sendall
        self.connect()

    def connect(self):
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.connect((self.host, self.port))
            self.sock.setblocking(False)
        except:
            logging.exception(f"Failed to connect Ninja API endpoint at {self.host}:{self.port}")
            return
        self.connected = True
        self.hb_thread = threading.Thread(target=self.send_heartbeats)
        self.hb_thread.start()

    def disconnect(self):
        if not self.connected:
            return
        self.connected = False
        self.hb_thread.join()
        self.sock.close()

    def send_msg(self, container: NinjaApiMessages_pb2.MsgContainer):
        if not self.connected:
            logging.error("Not connected. Cannot send messages.")
            return
        serialized_container = container.SerializeToString()
        framing = struct.pack("i", len(serialized_container))
        with self.send_lock:
            try:
                self.sock.sendall(framing + serialized_container)
            except:
                logging.exception("Failed sending message. Disconnecting client.")
                self.disconnect()

    def recv_msg(self) -> Optional[NinjaApiMessages_pb2.MsgContainer]:
        if not self.connected:
            print("Not connected. Cannot receive messages.")
            return None
        frame_size = struct.calcsize("i")
        try:
            while len(self.frame) < frame_size:
                buffer = self.sock.recv(frame_size - len(self.frame))
                if not buffer:
                    logging.info("Server disconnected")
                    self.disconnect()
                    return None
                self.frame += buffer
            msg_size = struct.unpack("i", self.frame)[0]
            while len(self.msg) < msg_size:
                buffer = self.sock.recv(msg_size - len(self.msg))
                if not buffer:
                    logging.info("Server disconnected")
                    self.disconnect()
                    return None
                self.msg += buffer
            container = NinjaApiMessages_pb2.MsgContainer()
            container.ParseFromString(self.msg)
            self.frame = b""
            self.msg = b""
            return container
        except BlockingIOError:
            return None
        except:
            logging.exception("Receive error. Disconnecting client.")
            self.disconnect()
            return None

    def send_heartbeats(self):
        container = NinjaApiMessages_pb2.MsgContainer()
        container.header.msgType = NinjaApiMessages_pb2.Header.HEARTBEAT
        last_send_time = time.time()
        sleep_time = 0.001 # 1 millisecond
        while self.connected:
            while (time.time() - last_send_time) + sleep_time < 5:
                if not self.connected:
                    return
                else:
                    time.sleep(sleep_time)
            logging.info("Sending HEARTBEAT")
            self.send_msg(container)
            last_send_time = time.time()

    @abstractmethod
    def run(self):
        pass
