import logging
import threading
import clients

# from trading_client import TradingClient
# from trading_client_threading import TradingClient
from trading_client_orders_new import TradingClient
from positions_client_new import PositionsClient
import flatten_and_close
import algo_monkey
from config import settings


def run_clients():
    logging.basicConfig(
        format="%(levelname)s - %(asctime)s: %(message)s", level=logging.INFO
    )
    programs = []
    threads = []

    # ADD CLIENTS
    if settings.positions_access_token:
        logging.info(f"Positions Client Added")
        clients.positionClient = PositionsClient()
        programs.append(clients.positionClient)
    if settings.trading_access_token:
        logging.info(f"Trading Client Added")
        clients.tradingClient = TradingClient()
        programs.append(clients.tradingClient)
    for client in programs:
        thread = threading.Thread(target=client.run, daemon=True)
        thread.start()
        threads.append(thread)

    # ADD ALGOS
    clients.algoMonkey = algo_monkey.Monkey()
    thread = threading.Thread(target=clients.algoMonkey.run, daemon=True)
    thread.start()
    threads.append(thread)

    # ADD FLATTEN
    thread = threading.Thread(target=flatten_and_close.run, daemon=True)
    thread.start()
    threads.append(thread)

    # JOIN ALL THREADS
    for thread in threads:
        thread.join()


if __name__ == "__main__":
    run_clients()
