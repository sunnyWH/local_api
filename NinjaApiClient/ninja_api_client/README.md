# CLASSES AND METHODS

## ninja_api_client.py (parent)

| Function                           | Args | Description |
| ------                             | ------   | ------      |
| `__init__`                        | self, host, port | initializes the connection to the host port   |
| `connect`           | self | attempts to connect our instance to the port    |
| `disconnect`           | self | attempts to disconnect our instance from the port |
| `send_msg`           | self, container | when connected, attempts to send message in container |
| `recv_msg`           | self  | ensures container has buffer, reads then returns msg as a container |
| `send_heartbeats`           | self | when connected, sends a heartbeat out every 5 seconds by default|
| `run`                        | self | abstract, to be defined in child classes   |


## trading_client.py

This endpoint will connect directly to your ninja and allow you to query for both market data and submit orders. You can only have one of these per endpoint and must have a valid access token to connect.


## positions_client.py

This is a dedicated endpoint for position queries. It is ninja agnostic. When submitting a position query, you will submit it for accounts, not for a ninja. You can have multiple position connections but must have a unique access token for each connection you make.