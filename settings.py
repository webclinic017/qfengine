import pytz
import os

#! SETTINGS - Modify for YOUR Setup


TIMEZONE = pytz.timezone('America/New_York')

SUPPORTED = {
    'CURRENCIES': [
            'USD',
    ],
}

LOGGING = {
    'DATE_FORMAT': '%Y-%m-%d %H:%M:%S'
}


PRINT_EVENTS = True
def set_print_events(print_events=True):
    global PRINT_EVENTS
    PRINT_EVENTS = print_events



CSV_DIRECTORIES = {
    'PRICE': {
        'DAILY': "./csv/price/daily",
            },
    
    'FUNDAMENTAL':{
        'BALANCE_SHEET': "./csv/fundamental/balance_sheet",
        'INCOME_STATEMENT': "./csv/fundamental/income_statement",
        'CASH_FLOW_STATEMENT': "./csv/fundamental/cash_flow_statement",
    },

}

#---| Primarily for Securities Master Database
MYSQL_CREDENTIALS = {
            'user': 'YOUR USERNAME',
            'passwd': 'YOUR PASSWORD',
            'host': 'YOUR HOST (or localhost)',
            'db':'YOUR DATABASE'
}


#---| API settings
API = {
    'Alpaca':{
        'id': "YOUR KEY ID",
        'key': "YOUR SECRET KEY",
    },

    'IEX':{
        'id': "YOUR KEY ID",
        'key': "YOUR SECRET KEY",
    },

    'IEXSandbox':{
        'id': "YOUR KEY ID",
        'key': "YOUR SECRET KEY",
    },
    #--| add more if needed
}