import logging
import threading

# from trading_client import TradingClient
from trading_client_threading import TradingClient
from positions_client import PositionsClient
from config import settings


def run_clients():
    logging.basicConfig(
        format="%(levelname)s - %(asctime)s: %(message)s", level=logging.INFO
    )

    clients = []
    trading_client = TradingClient()
    clients.append(trading_client)
    if settings.positions_access_token:
        positions_client = PositionsClient()
        clients.append(positions_client)

    threads = []
    for client in clients:
        thread = threading.Thread(target=client.run, daemon=True)
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()


if __name__ == "__main__":
    run_clients()
