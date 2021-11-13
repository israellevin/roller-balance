'Tests for roller-balance server.'

# pylint: disable=unused-import
import pytest
# pylint: enable=unused-import

import data
import logs
import web

LOGGER = logs.logging.getLogger('roller.test')
ADDRESSES = [40*str(digit) for digit in range(10)]
INVALID_ADDRESS = ADDRESSES[0][:-1]


def initialize_test_database():
    'Initialize the database for testing.'
    assert data.DB_NAME[-5:] == '_test', f"will not run data tests on non test database {data.DB_NAME}"
    data.nuke_database_and_create_new_please_think_twice()

    # Check error handling.
    with pytest.raises(data.pymysql.MySQLError):
        with data.sql_connection() as sql:
            sql.execute('bad sql')


def test_data():
    'Test data access.'
    initialize_test_database()

    # Test deposits.
    assert data.get_balance(ADDRESSES[0]) == 0
    data.deposit(ADDRESSES[0], 1, 'fake transaction')
    assert data.get_balance(ADDRESSES[0]) == 1
    data.deposit(ADDRESSES[0], 10, 'fake transaction')
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
    assert data.settle('fake transaction') == dict(settled_transactions_count=3, unsettled_transaction_count=0)
    assert data.get_unsettled_withdrawals() == ()
    assert data.get_unsettled_withdrawals_aggregated_csv() == ''


def test_webserver():
    'Web server tests.'
    initialize_test_database()
    with web.APP.test_client() as client:
        balance_response = client.post('/get_balance', data=dict(address=ADDRESSES[0]))
        assert balance_response.json == dict(status=200, balance=0)
        assert balance_response.status == '200 OK'

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
        assert client.post('/settle', data=dict(transaction_hash='fake transaction')).status == '201 CREATED'
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

        for bad_argument in ['string', 1.1]:
            error_response = client.post('/deposit', data=dict(address=ADDRESSES[0], amount=bad_argument))
            assert error_response.status == '400 BAD REQUEST'
            LOGGER.info(bad_argument)
            LOGGER.info(f"argument amount has to be an integer, not a {type(bad_argument).__name__}")
            LOGGER.info(error_response.json['error_message'])
            assert error_response.json == dict(
                    status=400, error_name='ArgumentMismatch',
                    error_message='argument amount has to be an integer')

        for bad_amount in [0, -1]:
            error_response = client.post('/deposit', data=dict(address=ADDRESSES[0], amount=bad_amount))
            assert error_response.status == '400 BAD REQUEST'
            LOGGER.info(error_response.json)
            assert error_response.json == dict(
                    status=400, error_name='ArgumentMismatch',
                    error_message='argument amount must be larger than zero')

        assert client.get('/no_such_endpoint').status == '403 FORBIDDEN'


def test_logs():
    'Just for coverage.'
    web.logs.setup(suppress_loggers=['foo'])
