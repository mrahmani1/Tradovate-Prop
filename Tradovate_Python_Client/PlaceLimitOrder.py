#Place TV limit order
import requests
from GetAccessToken import getAccessToken
from config import *
import time
from typing import Optional
import logging

logger = logging.getLogger(__name__)


# Be careful with this one! It will submit an order!
# buysell must be either "Sell" or "Buy"
def placeLimitOrder(live: bool, account_id: int, symbol: str, qty: int, buysell: str, order_price: float, token: str) -> Optional[int]:
    """Places limit order.

    Args:
        live (bool): True = live, False = demo
        account_id (int): The Tradovate account ID.
        symbol (str): The instrument symbol.
        qty (int): Quantity of contracts.
        buysell (string): "Buy" or "Sell".
        order_price (float): Limit price for the order.
        token (string): User's access token.

    Returns:
        Optional[int]: orderId if successful, None otherwise.
    """

    header = {
        'Authorization': f'Bearer ${token}', 
        'Accept': 'application/json',
        'Content-Type': 'application/json'
            }

    if live:
        url = "https://live.tradovateapi.com/v1/order/placeorder"
    else:
        url = "https://demo.tradovateapi.com/v1/order/placeorder"

    order = {"accountSpec": USER_NAME,
            "accountId": account_id,
            "action": buysell,
            "symbol": symbol,
            "orderQty": qty,
            "price": order_price,
            "orderType": "Limit",
            "isAutomated": "true"
            }

    # Make request
    try:
        res = requests.post(url, headers=header, json=order)
        res.raise_for_status()

        content = res.json()
        orderId = content.get("orderId")

        if orderId:
            logger.info(f"Limit Order Placed! Symbol={symbol}, Qty={qty}, Price={order_price}, Action={buysell}, ID={orderId}")
            return int(orderId)
        else:
            logger.error(f"Limit Order failed: No orderId in response. Response: {content}")
            return None

    except requests.exceptions.RequestException as e:
        logger.error(f"Limit Order Request Failed: {e}")
        if e.response is not None:
            logger.error(f"Response Status: {e.response.status_code}")
            logger.error(f"Response Text: {e.response.text}")
        return None
    except Exception as e:
        logger.exception(f"Limit Order failed unexpectedly: {e}")
        return None


#token = getAccessToken(False)[0]
#acc_id_demo = 123456
#symbol_demo = 'MESU2'
#price_demo = 4000.0
#print(placeLimitOrder(False, acc_id_demo, symbol_demo, 1, "buy", price_demo, token))