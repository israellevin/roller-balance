'API definitions for flasgger.'

CONFIG = {
    'title': 'roller-balance API',
    'uiversion': 2,
    'specs_route': '/',
    'specs': [{
        'endpoint': '/',
        'route': '/apispec.json',
    }],
    'info': {
        'title': 'roller-balance API',
        'version': 0,
        'license': {
            'name': 'CC0',
            'url': 'https://creativecommons.org/publicdomain/zero/1.0/legalcode'
        },
        'description': 'An API from the roller game server to the roller-balance service.'
    }
}

GET_BALANCE = {
    'description': 'Get the roller balance of an ethereum address',
    'parameters': [
        {
            'name': 'address', 'description': 'The address queried',
            'in': 'formData', 'required': True, 'type': 'string'
        }
    ],
    'responses': {
        '200': {'description': 'The roller balance of the address'}
    }
}

TRANSFER = {
    'description': 'Transfer amount from source to target',
    'parameters': [
        {
            'name': 'source', 'description': 'The paying address',
            'in': 'formData', 'required': True, 'type': 'string'
        },
        {
            'name': 'target', 'description': 'The paid address',
            'in': 'formData', 'required': True, 'type': 'string'
        },
        {
            'name': 'amount', 'description': 'The amount transferred',
            'in': 'formData', 'required': True, 'type': 'integer'
        }
    ],
    'responses': {
        '201': {'description': 'The transfer was marked'}
    }
}

WITHDRAW = {
    'description': 'Withdraw an amount from the system',
    'parameters': [
        {
            'name': 'address', 'description': 'The withdrawing address',
            'in': 'formData', 'required': True, 'type': 'string'
        },
        {
            'name': 'amount', 'description': 'The amount withdrawn',
            'in': 'formData', 'required': True, 'type': 'integer'
        }
    ],
    'responses': {
        '201': {'description': 'The withdraw was marked'}
    }
}

GET_UNSETTLED_WITHDRAWALS = {
    'description': 'Get a list of all unsettled withdrawals',
    'tags': ['admin'],
    'parameters': [],
    'responses': {
        '200': {'description': 'A CSV list of unsettled withdrawals'}
    }
}

SETTLE = {
    'description': 'Get a list of all unsettled withdrawals',
    'tags': ['admin'],
    'parameters': [
        {
            'name': 'transaction_hash', 'description': 'The transaction hash of the settling multisend',
            'in': 'formData', 'required': True, 'type': 'string'
        }
    ],
    'responses': {
        '201': {'description': 'The number of settled withdrawals and the number of still unsettled withdrawals'}
    }
}

DEPOSIT = {
    'description': 'Fake a deposit of amount into address - debug only',
    'tags': ['debug'],
    'parameters': [
        {
            'name': 'address', 'description': 'The beneficiary address',
            'in': 'formData', 'required': True, 'type': 'string'
        },
        {
            'name': 'amount', 'description': 'The amount deposited',
            'in': 'formData', 'required': True, 'type': 'integer'
        }
    ],
    'responses': {
        '201': {'description': 'Deposit faked'}
    }
}
