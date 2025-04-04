#Gets list of all filled orders

import requests 
from GetAccessToken import getAccessToken
from config import *


def getPositions(live):
    """Gets the list of positions from Tradovate.

    Args:
        live (bool): True = live, False = demo

    Returns:
        List[Dict]: The list of position objects, or an empty list.
    """

    token = getAccessToken(live)[0]

    if live:
        url = "https://live.tradovateapi.com/v1/position/list"
    else:
        url = "https://demo.tradovateapi.com/v1/position/list"

    header = {
        'Authorization': f'Bearer ${token}'
        }

    res = requests.get(url, headers=header)
    content = res.json()
    
    # If no content there are no positions
    # if content == []: # <-- REMOVE BOOLEAN LOGIC
    #     return False

    # This is your net position
    # netPos = content[0]['netPos'] # <-- REMOVE BOOLEAN LOGIC
    
    # Zero net position means all your orders have been filled 
    # > 0 means you have a long position
    # < 0 means you have a short position
    # (I think that's how it goes, you can test to be sure, but this function works)
    # if netPos == 0: # <-- REMOVE BOOLEAN LOGIC
    #     return False
    # else:
    #     return True
    return content # <-- RETURN THE ACTUAL JSON DATA


# Test Here
#print(getPositions(False))