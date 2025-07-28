import csv
import os
import logging
from datetime import datetime


class TradingLogger:
    def __init__(self):
        self.current_date = datetime.today().date()
        self.filename = self._get_filename(self.current_date)
        self._ensure_file_exists()

    def _get_filename(self, date_obj):
        return f"trades/{date_obj}.csv"

    def _ensure_file_exists(self):
        # If file doesn't exist, create it with header
        if not os.path.exists(self.filename):
            with open(self.filename, mode="w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["time", "price", "quantity"])

    def log_trade(self, time, price, quantity, account="", tag=""):
        now = datetime.now()
        today = now.date()
        if today != self.current_date:
            # New day → new file
            self.current_date = today
            self.filename = self._get_filename(today)
            self._ensure_file_exists()

        with open(self.filename, mode="a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([time, price, quantity, account, tag])
            logging.info(f"Logged trade: {price}, {quantity}, {account}, {tag}")
