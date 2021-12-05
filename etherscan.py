'Etherscan blockchain services for roller-balance.'
import json
import logging
import os

import requests

LOGGER = logging.getLogger('roller.etherscan')
ETHERSCAN_API_KEY = os.environ['ROLLER_ETHERSCAN_API_KEY']
ETHERSCAN_API = 'https://api-ropsten.etherscan.io/api'
ETHERSCAN_HEADERS = {'User-Agent': 'Mozilla/5.0'}


class EtherscanError(Exception):
    'An error when fetching data from etherscan.'
    def __init__(self, message, data):
        super().__init__(message)
        self.data = data


def call(module, action, **kwargs):
    'Call etherscan API and return a parsed response.'
    response = None
    try:
        response = requests.post(ETHERSCAN_API, headers=ETHERSCAN_HEADERS, data=dict(
            apikey=ETHERSCAN_API_KEY, module=module, action=action, **kwargs))
        return response.json()['result']
    except (KeyError, ValueError, json.decoder.JSONDecodeError, requests.exceptions.RequestException):
        LOGGER.exception('etherscan error')
        raise EtherscanError(f"failed getting {module}.{action}", data=dict(response=response)) from None


def get_latest_block_number():
    'Get the number of the latest block.'
    block_number_hex = call('proxy', 'eth_blockNumber')
    try:
        return int(block_number_hex, 16)
    except ValueError:
        LOGGER.exception(f"got bad block number - {block_number_hex}")
        raise EtherscanError('bad last block', data=dict(block_number_hex=block_number_hex)) from None


def get_deposits(address, start_block, end_block):
    'Get all ether payments made to address.'
    LOGGER.info(f"scanning from {start_block} to {end_block}")
    tx_list = call(
        'account', 'txlist', address=f"0x{address}", startblock=start_block, endblock=end_block, sort='asc')
    return [dict(
        source=transaction['from'][2:], amount=int(transaction['value']),
        block_number=int(transaction['blockNumber']), transaction=transaction['hash'][2:]
    ) for transaction in tx_list if (
        transaction['to'].lower() == f"0x{address.lower()}" and
        transaction['isError'] == '0' and
        int(transaction['value'], 10) > 0)]


def get_payments(target_address, transaction_hash):
    'Get a list of all payments made in a multisender call.'
    transaction = call('proxy', 'eth_getTransactionByHash', txhash=f"0x{transaction_hash}")
    if transaction['from'].lower() != f"0x{target_address.lower()}":
        LOGGER.error(f"transaction sender is {transaction['from']} and not {target_address} as specified")
        return []
    return [
        dict(address=internal_call['to'][2:], amount=int(internal_call['value']))
        for internal_call in call('account', 'txlistinternal', txhash=f"0x{transaction_hash}")]
