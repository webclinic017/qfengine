from qfengine.data.vendor_api.Alpaca.alpaca import Alpaca
import os



(os.environ["ALPACA_ID"],
os.environ["ALPACA_KEY"]) = open(
                                os.path.join(
                                            os.path.dirname(os.path.realpath(__file__)),
                                            'api_key.txt'
                                            )
                                ).read().split('\n')


info = {
    'name': 'Alpaca',
    'website_url':'https://alpaca.markets/',
    'api_endpoint_url': 'https://api.alpaca.markets',
       }