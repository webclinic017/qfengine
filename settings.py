import pytz


SUPPORTED = {
    'CURRENCIES': [
        'USD', 'GBP', 'EUR'
    ],
    'FEE_MODEL': {
        'ZeroFeeModel': 'qstrader.broker.fee_model.zero_fee_model'
    }
}

LOGGING = {
    'DATE_FORMAT': '%Y-%m-%d %H:%M:%S'
}

TIMEZONE = pytz.timezone('America/New_York')


PRINT_EVENTS = True

MYSQL_CREDENTIALS = {
            'user': 'root',
            'passwd': 'Immortality1015!',
            'host': 'localhost',
            'db':'equities'
}




def set_print_events(print_events=True):
    global PRINT_EVENTS
    PRINT_EVENTS = print_events