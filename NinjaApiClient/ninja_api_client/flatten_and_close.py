import clients
import logging
import time

# from clients import algoMonkey, algoRB


def run():
    logging.info("Flattening Client Added")
    while True:
        if input().strip().lower() == "q":
            clients.positionClient.get_all_positions()
            time.sleep(0.1)
            positions = clients.positionClient.positions
            for entry in positions.keys():
                account = entry[0]
                exchange = entry[1]
                product = entry[2]
                if positions.get(entry) > 0:
                    clients.tradingClient.order(
                        account=account,
                        product=product,
                        price=clients.tradingClient.latest_bid.get(product),
                        qty=-positions.get(entry),
                        worker="w",
                        exchange=exchange,
                        tag="FLATTEN_PROGRAMCLOSE",
                        log=True,
                    )
                elif positions.get(entry) < 0:
                    clients.tradingClient.order(
                        account=account,
                        product=product,
                        price=clients.tradingClient.latest_ask.get(product),
                        qty=-positions.get(entry),
                        worker="w",
                        exchange=exchange,
                        tag="FLATTEN_PROGRAMCLOSE",
                        log=True,
                    )

            if clients.positionClient is not None:
                logging.info("POSITIONS_CLIENT: Disconnecting...")
                clients.positionClient.disconnect()
            else:
                logging.warning("POSITIONS_CLIENT not set!")

            if clients.tradingClient is not None:
                logging.info("TRADING_CLIENT: Disconnecting...")
                clients.tradingClient.disconnect()
            else:
                logging.warning("TRADING_CLIENT not set!")

            if clients.algoMonkey is not None:
                clients.algoMonkey.disconnect()
                logging.info("ALGO_MONKEY: Disconnecting...")
            if clients.algoRB is not None:
                clients.algoRB.disconnect()
                logging.info("ALGO_RB: Disconnecting...")

            logging.info("FLATTEN_CLIENT: Disconnecting...")

            break
