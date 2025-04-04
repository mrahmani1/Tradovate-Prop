#Place TV market order
import requests
import time
from GetAccessToken import getAccessToken
from config import *
from typing import Optional
import logging

logger = logging.getLogger(__name__)


# Be careful!
# buysell must be either "Sell" or "Buy"
def placeMarketOrder(live: bool, account_id: int, symbol: str, qty: int, buysell: str, token: str) -> Optional[int]:
    """Places market order.

    Args:
        live (bool): True = live, False = demo
        account_id (int): The Tradovate account ID to place the order on.
        symbol (str): The instrument symbol (e.g., 'ESU2').
        qty (int): Quantity of contracts.
        buysell (string): "Buy" or "Sell".
        token (string): User's access token.

    Returns:
        Optional[int]: orderId if successful, None otherwise.
    """

    header = {
        'Authorization': f'Bearer ${token}', 
        'Accept': 'application/json',
        'Content-Type': 'application/json' # Added Content-Type based on LimitOrder script
            }

    if live:
        url = "https://live.tradovateapi.com/v1/order/placeorder"
        # accId = LIVE_ACCOUNT_ID # <-- REMOVE HARDCODED
    else:
        url = "https://demo.tradovateapi.com/v1/order/placeorder"
        # accId = DEMO_ACCOUNT_ID # <-- REMOVE HARDCODED

    # Use provided account_id and symbol
    order = {"accountSpec": USER_NAME, # Keep USER_NAME from config or pass as arg?
            "accountId": account_id, # <-- USE ARGUMENT
            "action": buysell,
            "symbol": symbol, # <-- USE ARGUMENT
            "orderQty": qty, # <-- USE ARGUMENT (consistent naming)
            "orderType": "Market",
            "isAutomated": "true"
            }

    # Make request
    try:
        # res = requests.post(url, headers=header, data=order) # <-- Pass data as JSON
        res = requests.post(url, headers=header, json=order) # Use json= parameter
        res.raise_for_status() # Raise exception for bad status codes (4xx or 5xx)

        content = res.json()
        orderId = content.get("orderId") # Use .get() for safety

        if orderId:
            logger.info(f"Market Order Placed! Symbol={symbol}, Qty={qty}, Action={buysell}, ID={orderId}")
            return int(orderId)
        else:
            logger.error(f"Market Order failed: No orderId in response. Response: {content}")
            return None

    except requests.exceptions.RequestException as e:
        logger.error(f"Market Order Request Failed: {e}")
        if e.response is not None:
            logger.error(f"Response Status: {e.response.status_code}")
            logger.error(f"Response Text: {e.response.text}")
        return None
    except Exception as e:
        logger.exception(f"Market Order failed unexpectedly: {e}")
        return None


#token = getAccessToken(False)[0]
#acc_id_demo = 123456 # Get from config or GetAccountList
#symbol_demo = 'MESU2'
#placeMarketOrder(False, acc_id_demo, symbol_demo, 1, "Buy", token)