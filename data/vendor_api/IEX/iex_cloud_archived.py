import os, os.path
import pandas as pd
import numpy as np
import datetime
import logging
import time

#----| Credits to an existing wrapper made by ___
import iexfinance
from iexfinance.stocks import get_historical_data,get_historical_intraday
from iexfinance.stocks import Stock
import iexfinance.data_apis as dataAPI


logger = logging.getLogger(__name__)



#--------------| <-IEX Cloud API-> Private Paid Key (keep private duh)
class IEX(object):
#------------------------#
    name = 'IEX'
    website_url = 'https://iexcloud.io/'
    api_endpoint_url = 'https://cloud.iexapis.com/'
    api_key = open(os.path.join(os.path.dirname(os.path.realpath(__file__)),"api_key.txt")).read().split("\n")[0]

    def __init__(self,sandbox=False,pandasOutput=True):
        self._path = os.path.dirname(os.path.realpath(__file__))
        self._dbPath = {
                        "1D":os.path.join(self._path,"1D"),
                        "1Min":os.path.join(self._path,"1Min"),
                        "Financials":os.path.join(self._path,"Financials"),
                        "FundOwnerships":os.path.join(self._path,"FundOwnerships"),
                        }
        self._sandbox = sandbox
        self._pandasOutput = pandasOutput
        self._overwriteMode_()

        self._calendar = pd.read_csv(os.path.join(self._path,'market_calendar.csv')).set_index('date')
        self._calendar.index = pd.DatetimeIndex(self._calendar.index)
        self._calendar = self._calendar[self._calendar.index <= pd.Timestamp.now()]



    def _overwriteMode_(self):
        self._token = os.environ["IEX_TOKEN"] = (
                                                open(os.path.join(self._path,"api_key.txt")).read().split("\n")[0]
                                                        if not self._sandbox
                                                        else open(os.path.join(self._path,"api_key_sandbox.txt")).read().split("\n")[0]
                                                )

        self._version = os.environ["IEX_API_VERSION"] = "iexcloud-v1" if not self._sandbox else "iexcloud-sandbox"
        self._outputFormat = os.environ["IEX_OUTPUT_FORMAT"] = "pandas" if self._pandasOutput else "json"

    @property
    def get_time_series(self):
        self._overwriteMode_()
        return dataAPI.get_time_series
    @property
    def get_data_points(self):
        self._overwriteMode_()
        return dataAPI.get_data_points

#----------------------------------| API Functions

    @staticmethod
    def _prepare_date_format(datestamp): #need to be in datetime.date
        try:
            d = pd.Timestamp(datestamp).date() # Works most cases
            return d
        except:
            try:
                d = datestamp.date()
                return d
            except:
                raise Exception("Invalid datestamp provided %s" %str(datestamp))
        
    
    def get_barset(self,symbols,
                        timeframe=None,
                        start_date=None,
                        end_date=None
    ): #---| end_date = today/latest available
        self._overwriteMode_()
        if isinstance(symbols,str):
            symbols = [symbols]
        if timeframe is None:
            timeframe = "1D"
        if end_date is None:
            end_date = self._calendar.index[-1]
        if start_date is None:
            start_date = (pd.Timestamp(end_date) - pd.Timedelta("5y"))

        end_ = IEX._prepare_date_format(end_date)
        start_ = IEX._prepare_date_format(start_date)
        assert end_ >= start_


        df = pd.DataFrame()
        if timeframe == "1D":
            df = get_historical_data(symbols,start=start_,end=end_)
        elif timeframe == "1Min":
            assert isinstance(symbols,str), "only 1 date at a time possible for IEX Cloud"
            start_ = (start_date if (isinstance(start_date,datetime.datetime))
                        else (start_date.to_pydatetime() if isinstance(start_date,pd.Timestamp)
                                else (pd.Timestamp(start_date).to_pydatetime() if isinstance(start_date,str) else 
                                                                (pd.Timestamp.now() - pd.Timedelta("5y")).to_pydatetime())))
            df = get_historical_intraday(symbols,date=start_)
        
        df.index = pd.DatetimeIndex(df.index.date)
        if len(symbols) == 1 and (not isinstance(df.columns, pd.MultiIndex)):
            multi_cols = [
                        np.array([symbols[0] for _ in range(df.shape[1])]),
                        np.array(list(df.columns))
                         ]
            df.columns = multi_cols
        
        assert isinstance(df.columns,pd.MultiIndex)
        df.columns.names = ('symbols','columns')

        return df
            
    def get_price(self,symbol):
        self._overwriteMode_()
        return (Stock(symbol).get_price()).iloc[0,0]
    

    def get_financials(self,symbol:str,form = "10-K",last=1):
        self._overwriteMode_()
        assert form in ["10-K","10-Q"]
        financials = self.get_time_series('REPORTED_FINANCIALS',symbol,form,last=last).T.sort_index()
        if not financials.empty:
            financials.index = range(len(financials))
        return financials



class IEXSandbox(IEX):
    name = 'IEXSandbox'
    website_url = 'https://iexcloud.io/'
    api_endpoint_url = 'https://sandbox.iexapis.com'
    api_key = open(os.path.join(os.path.dirname(os.path.realpath(__file__)),"api_key_sandbox.txt")).read().split("\n")[0]
    def __init__(self):
        super().__init__(sandbox=True)
    
