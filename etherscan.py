'Etherscan blockchain services for roller-balance.'
import json
import logging
import os

import requests

import logs

LOGGER = logging.getLogger('roller.etherscan')
logs.setup()

ETHERSCAN_API_KEY = os.environ['ROLLER_ETHERSCAN_API_KEY']
ETHERSCAN_API = 'https://api-ropsten.etherscan.io/api'
ETHERSCAN_HEADERS = {'User-Agent': 'Mozilla/5.0'}


class EtherscanError(Exception):
    'An error when fetching data from etherscan.'
    def __init__(self, message, data):
        super().__init__(message)
        self.data = data


def get_latest_block_number():
    'Get the number of the latest block.'
    etherscan_response = requests.post(ETHERSCAN_API, headers=ETHERSCAN_HEADERS, data=dict(
        module='proxy', action='eth_blockNumber', apikey=ETHERSCAN_API_KEY))
    try:
        etherscan_response = etherscan_response.json()
        return int(etherscan_response['result'], 16)
    except (json.decoder.JSONDecodeError, KeyError, ValueError):
        raise EtherscanError('failed getting latest block', data=dict(etherscan_response=etherscan_response)) from None


def get_ether_payments(address, start_block, end_block):
    'Get all ether payments made to address.'
    LOGGER.info(f"scanning from {start_block} to {end_block}")
    address = f"0x{address}"
    etherscan_response = requests.post(ETHERSCAN_API, headers=ETHERSCAN_HEADERS, data=dict(
        module='account', action='txlist', sort='asc', apikey=ETHERSCAN_API_KEY,
        address=address, startblock=start_block, endblock=end_block))
    try:
        tx_list = etherscan_response.json()
    except json.decoder.JSONDecodeError:
        raise EtherscanError('failed getting payments', data=dict(etherscan_response=etherscan_response.text)) from None
    if tx_list['status'] == '0':  # pragma: no cover
        return []
    if tx_list['status'] != '1':  # pragma: no cover
        raise EtherscanError('failed getting payments', data=dict(tx_list=tx_list))

    return [dict(
        source=transaction['from'][2:], amount=int(transaction['value']),
        block_number=int(transaction['blockNumber']), transaction=transaction['hash'][2:]
    ) for transaction in tx_list['result'] if (
        transaction['to'].lower() == address.lower() and
        transaction['isError'] == '0' and
        int(transaction['value'], 10) > 0)]
