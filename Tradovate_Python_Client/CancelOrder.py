# Cancels an order

import requests
from typing import Optional, Dict, Any
import logging

logger = logging.getLogger(__name__)


def cancelOrder(live: bool, order_id: int, token: str) -> Optional[Dict[str, Any]]:
    """Cancels an order.

    Args:
        live (bool): True = live acc, False = demo acc
        order_id (int): Id of order you want to cancel.
        token (string): user's access token.

    Returns:
        Optional[Dict[str, Any]]: A dictionary containing the original 'orderId' if successful, None otherwise.
    """
    if live:
        url = "https://live.tradovateapi.com/v1/order/cancelorder"
    else:
        url = "https://demo.tradovateapi.com/v1/order/cancelorder"

    header = {
        'Authorization': f'Bearer ${token}',
        'Accept': 'application/json',
        'Content-Type': 'application/json'
            }

    data = {
        "orderId": order_id,
        "isAutomated": "true"
    }

    # Send request!
    try:
        res = requests.post(url, json=data, headers=header)
        res.raise_for_status()

        logger.info(f"Order {order_id} cancel request sent successfully.")
        response_data = res.json()
        logger.debug(f"CancelOrder response: {response_data}")
        return {"orderId": order_id}

    except requests.exceptions.RequestException as e:
        logger.error(f"Order Cancel Request Failed (ID: {order_id}): {e}")
        if e.response is not None:
            logger.error(f"Response Status: {e.response.status_code}")
            logger.error(f"Response Text: {e.response.text}")
        return None
    except Exception as e:
        logger.exception(f"Order Cancel failed unexpectedly (ID: {order_id}): {e}")
        return None

