import pytz


SUPPORTED = {
    'CURRENCIES': [
        'USD', 'GBP', 'EUR'
            ],
    }
}

LOGGING = {
    'DATE_FORMAT': '%Y-%m-%d %H:%M:%S'
}

TIMEZONE = pytz.timezone('America/New_York')


PRINT_EVENTS = True

#---| PROVIDE MySQL Database Credentials if using MySQL Data Source(s)
MYSQL_CREDENTIALS = {
            'user': 'root',
            'passwd': 'password',
            'host': 'localhost',
            'db':'database',
}




def set_print_events(print_events=True):
    global PRINT_EVENTS
    PRINT_EVENTS = print_events
