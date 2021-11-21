'Tests for roller-balance server.'
import uuid

# pylint: disable=unused-import
import pytest
# pylint: enable=unused-import

import data
import etherscan
import logs
import web

LOGGER = logs.logging.getLogger('roller.test')
ADDRESSES = [40*str(digit) for digit in range(10)]


def initialize_test_database():
    'Initialize the database for testing.'
    assert data.DB_NAME[-5:] == '_test', f"will not run data tests on non test database {data.DB_NAME}"
    data.nuke_database_and_create_new_please_think_twice()


def test_data_etherscan():
    'Test integration of data with etherscan module.'
    initialize_test_database()
    assert data.get_balance(data.SAFE) == 0
    data.scan_for_deposits()
    assert data.get_balance(data.SAFE) == -4*10**18

    # To make sure we hit the same block.
    data.scan_for_deposits()
    data.scan_for_deposits()

    # Make sure we hit old data.
    with data.sql_connection() as sql:
        sql.execute('UPDATE deposit_scans SET end_block = end_block + 1000')
    data.scan_for_deposits()

    # Create a duplicate transaction.
    initialize_test_database()
    data.deposit('fake', 1, '2735f031e4f57f7b1644d6146bafa096f4e6723250f2270b9a48d1ffd60e93e1')
    with pytest.raises(data.ScanError):
        data.scan_for_deposits()


def fake_transaction_hash():
    'Create a fake transaction hash.'
    return f"fake-{uuid.uuid4().hex}{uuid.uuid4().hex}"[:64]


def test_data():
    'Test data access.'
    initialize_test_database()

    # Check error handling.
    with pytest.raises(data.pymysql.MySQLError):
        with data.sql_connection() as sql:
            sql.execute('bad sql')

    # Test deposits.
    assert data.get_balance(ADDRESSES[0]) == 0
    data.deposit(ADDRESSES[0], 1, fake_transaction_hash())
    assert data.get_balance(ADDRESSES[0]) == 1
    data.deposit(ADDRESSES[0], 10, fake_transaction_hash())
    assert data.get_balance(ADDRESSES[0]) == 11

    # Test transfers.
    assert data.get_balance(ADDRESSES[1]) == 0
    data.transfer(ADDRESSES[0], ADDRESSES[1], 2)
    assert data.get_balance(ADDRESSES[0]) == 9
    assert data.get_balance(ADDRESSES[1]) == 2
    with pytest.raises(data.InsufficientFunds):
        data.transfer(ADDRESSES[0], ADDRESSES[1], 10)

    # Test withdraw.
    with pytest.raises(data.InsufficientFunds):
        data.withdraw(ADDRESSES[0], 100)
    data.withdraw(ADDRESSES[0], 3)
    assert data.get_balance(ADDRESSES[0]) == 6
    assert data.get_unsettled_withdrawals() == [
        dict(idx=4, address=ADDRESSES[0], amount=3)]
    assert data.get_unsettled_withdrawals_aggregated_csv() == f"{ADDRESSES[0]}, 3"
    data.withdraw(ADDRESSES[0], 4)
    assert data.get_balance(ADDRESSES[0]) == 2
    assert data.get_unsettled_withdrawals() == [
        dict(idx=4, address=ADDRESSES[0], amount=3),
        dict(idx=5, address=ADDRESSES[0], amount=4)]
    assert data.get_unsettled_withdrawals_aggregated_csv() == f"{ADDRESSES[0]}, 7"
    data.withdraw(ADDRESSES[1], 2)
    assert data.get_balance(ADDRESSES[1]) == 0
    assert data.get_unsettled_withdrawals() == [
        dict(idx=4, address=ADDRESSES[0], amount=3),
        dict(idx=5, address=ADDRESSES[0], amount=4),
        dict(idx=6, address=ADDRESSES[1], amount=2)]
    assert data.get_unsettled_withdrawals_aggregated_csv() == f"{ADDRESSES[0]}, 7\n{ADDRESSES[1]}, 2"
    assert data.settle(fake_transaction_hash()) == dict(settled_transactions_count=3, unsettled_transaction_count=0)
    assert data.get_unsettled_withdrawals() == ()
    assert data.get_unsettled_withdrawals_aggregated_csv() == ''


def test_webserver():
    'Web server tests.'
    initialize_test_database()
    web.DEBUG = True
    with web.APP.test_client() as client:
        balance_response = client.post('/get_balance', data=dict(address=ADDRESSES[0]))
        assert balance_response.status == '200 OK'
        assert balance_response.json == dict(status=200, balance=0)

        deposit_response = client.post('/deposit', data=dict(address=ADDRESSES[0], amount=100))
        assert deposit_response.status == '201 CREATED'
        assert client.post('/get_balance', data=dict(address=ADDRESSES[0])).json['balance'] == 100

        transfer_response = client.post('/transfer', data=dict(
            source=ADDRESSES[0], target=ADDRESSES[1],  amount=101))
        assert transfer_response.status == '400 BAD REQUEST'
        assert transfer_response.json == dict(
                status=400, error_name='InsufficientFunds',
                error_message=f"address {ADDRESSES[0]} has less than 101 rollers")
        transfer_response = client.post('/transfer', data=dict(
            source=ADDRESSES[0], target=ADDRESSES[1],  amount=10))
        assert transfer_response.status == '201 CREATED'
        assert client.post('/get_balance', data=dict(address=ADDRESSES[0])).json['balance'] == 90
        assert client.post('/get_balance', data=dict(address=ADDRESSES[1])).json['balance'] == 10

        withdraw_response = client.post('/withdraw', data=dict(address=ADDRESSES[0], amount=20))
        assert withdraw_response.status == '201 CREATED'
        assert client.post('/get_balance', data=dict(address=ADDRESSES[0])).json['balance'] == 70

        assert client.post('/get_unsettled_withdrawals').status == '200 OK'
        assert client.post('/get_unsettled_withdrawals').json['unsettled_withdrawals'] != ''
        assert client.post('/settle', data=dict(transaction_hash=64*'0')).status == '201 CREATED'
        assert client.post('/get_unsettled_withdrawals').json['unsettled_withdrawals'] == ''

        error_response = client.post('/get_balance')
        assert error_response.status == '400 BAD REQUEST'
        assert error_response.json == dict(
            status=400, error_name='ArgumentMismatch',
            error_message='request does not contain arguments(s): address')

        error_response = client.post('/get_balance', data=dict(bad_argument='stam', address=ADDRESSES[0]))
        assert error_response.status == '400 BAD REQUEST'
        assert error_response.json == dict(
            status=400, error_name='ArgumentMismatch',
            error_message='request contain unexpected arguments(s): bad_argument')

        for bad_amount in ['string', 1.1]:
            error_response = client.post('/deposit', data=dict(address=ADDRESSES[0], amount=bad_amount))
            assert error_response.status == '400 BAD REQUEST'
            assert error_response.json == dict(
                status=400, error_name='ArgumentMismatch',
                error_message='argument amount has to be an integer')

        for bad_amount in [0, -1]:
            error_response = client.post('/deposit', data=dict(address=ADDRESSES[0], amount=bad_amount))
            assert error_response.status == '400 BAD REQUEST'
            assert error_response.json == dict(
                status=400, error_name='ArgumentMismatch',
                error_message='argument amount must be larger than zero')

        for bad_address, error_message in [
            (f"{ADDRESSES[0][:-1]}", 'argument address must be 40 characters long'),
            (f"{ADDRESSES[0][:-1]}g", 'argument address is not a hex string')
        ]:
            error_response = client.post('/get_balance', data=dict(address=bad_address))
            assert error_response.status == '400 BAD REQUEST'
            assert error_response.json == dict(
                status=400, error_name='ArgumentMismatch',
                error_message=error_message)

        for bad_tx_hash, error_message in [
            (63*'0', 'argument transaction_hash must be 64 characters long'),
            (64*'g', 'argument transaction_hash is not a hex string')
        ]:
            error_response = client.post('/settle', data=dict(transaction_hash=bad_tx_hash))
            assert error_response.status == '400 BAD REQUEST'
            assert error_response.json == dict(
                status=400, error_name='ArgumentMismatch',
                error_message=error_message)

        assert client.post('/settle', data=dict(transaction_hash=64*'0')).status == '201 CREATED'

        error_response = client.get('/five_hundred')
        for reason in ['response', 'exception']:
            error_response = client.post('/five_hundred', data=dict(reason=reason))
            assert error_response.status == '500 INTERNAL SERVER ERROR'

        assert client.get('/no_such_endpoint').status == '403 FORBIDDEN'


def test_prod_webserver():
    'Some tests have to be run in a non debug node, for coverage.'
    initialize_test_database()
    web.DEBUG = False
    with web.APP.test_client() as client:
        error_response = client.post('/deposit', data=dict(address=ADDRESSES[0], amount=10))
        assert error_response.status == '403 FORBIDDEN'
        error_response = client.post('/five_hundred', data=dict(reason='exception'))
        assert error_response.status == '403 FORBIDDEN'


def test_etherscan():
    'Test etherscan module.'
    assert etherscan.get_latest_block_number() > 0

    LOGGER.info(etherscan.get_ether_payments('7f6041155c0DB03eB2b49AbF2D61b370B4253Ef7', 9833390, 9833490))
    assert etherscan.get_ether_payments('7f6041155c0DB03eB2b49AbF2D61b370B4253Ef7', 9833390, 9833490) == [{
        'source': '81b7e08f65bdf5648606c89998a9cc8164397647', 'amount': 1000000000000000000,
        'block_number': 9833391, 'transaction': 'f288c15ac4741b246922abd0bbc06727d7958a03a5e251e6449af034866d71c1'
    }, {
        'source': '81b7e08f65bdf5648606c89998a9cc8164397647', 'amount': 1000000000000000000,
        'block_number': 9833392, 'transaction': '2f741098a7c33d898b16de7ec1a8bf1658a1983806e89a13bcceb40aee878ba3'}]

    original_headers = etherscan.ETHERSCAN_HEADERS
    etherscan.ETHERSCAN_HEADERS = {}
    with pytest.raises(etherscan.EtherscanError):
        etherscan.get_latest_block_number()
    with pytest.raises(etherscan.EtherscanError):
        etherscan.get_ether_payments('7f6041155c0DB03eB2b49AbF2D61b370B4253Ef7', 9833390, 9833490)
    etherscan.ETHERSCAN_HEADERS = original_headers


def test_logs():
    'Just for coverage.'
    web.logs.setup(suppress_loggers=['foo'])
