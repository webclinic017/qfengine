from qfengine.data.price.price_database import CSVPriceDataSource
from qfengine.asset import assetClasses
from qfengine import settings
from typing import Union, List

import pandas as pd
import numpy as np
import os
import functools
import pytz


class DailyPriceCSV(CSVPriceDataSource):

    def __init__(
                 self,
                 asset_type:assetClasses,
                 csv_dir:str,
                    csv_symbols:List[str] = None,
                    min_start_date = None,
                    init_to_latest:bool = True,
                    adjust_prices:bool = False,
    ):
        super().__init__(csv_dir, csv_symbols)
        self.asset_type = asset_type
        self.adjust_prices = adjust_prices
        self.min_start_date = min_start_date

        self.symbols_asset_data = {}
        self.symbols_price_data = {}
        self.latest_symbols_price_data = {}
        self.asset_bid_ask_frames = {}
        self.csv_combined_index = None
        self._all_latest_loaded = False

        self.initialize_price_database(min_start_date, init_to_latest)
        
        
    def initialize_price_database(self,
                                  min_start_date = None,
                                  init_to_latest:bool=False
    ):
        min_start = min_start_date if min_start_date is not None else (pd.Timestamp.now(tz=settings.TIMEZONE) - pd.Timedelta('1D'))

        combinedIndex = None
        symbols_data = {}
        latest_symbols_data = {}
        for s in self.csv_symbols:
            s_df = self._csv_to_df(self._csv_file_from_symbol(s))
            s_df = s_df.set_index(s_df.index.tz_localize(settings.TIMEZONE))
            if s_df.index[0] <= min_start:
                if combinedIndex is None:
                    combinedIndex = s_df.index
                else:
                    combinedIndex.union(s_df.index)
                symbols_data[s] = s_df
                latest_symbols_data[s] = []
        assert combinedIndex is not None

        asset_bid_ask_frames = {
                            asset_symbol : self._assets_price_data_to_bid_ask(
                                                                    df.reindex(combinedIndex,method='pad')
                                                                             ) for asset_symbol,df in symbols_data.items()
                                }
        #---| INITIALIZE EVERYTHING


        if not init_to_latest:#---| usually for backtesting purposes
            self.symbols_price_data = {s:s_df.reindex(combinedIndex, method='pad').iterrows() for s,s_df in symbols_data.items()}
            self._all_latest_loaded = False
        else:
            self.symbols_price_data = {}
            self._all_latest_loaded = True
        
        self.latest_symbols_price_data = latest_symbols_data
        self.asset_bid_ask_frames = asset_bid_ask_frames
        self.csv_combined_index = combinedIndex
        self.csv_symbols = list(symbols_data.keys())


        
        
    def get_latest_price_data(self, symbol, N=1):
        try:
            dat = self.latest_symbols_price_data[symbol]
        except KeyError:
            raise Exception("symbol %s does not exist in current csv database" %symbol)
        if len(dat) == 0 and self._all_latest_loaded:
            full_df = self._csv_to_df(self._csv_file_from_symbol(symbol)).reindex(
                                                                            index=self.csv_combined_index,
                                                                            method='pad'
                                                                            )
            full_df = full_df.sort_index().tail(N)                                                            
            return [
                tuple([symbol, idx,
                                d[0], d[1], d[2], d[3], d[4]
                      ]) for idx,d in zip(full_df.index, full_df.values)
                  ]
        
        return dat[-N:]

    def update_price_data(self):
        if self._all_latest_loaded:
            return
        new_loaded = []
        for s in self.csv_symbols:
            try:
                dat = next(self._get_new_data(s))
            except StopIteration:
                pass
            else:
                new_loaded.append(s)
                if dat is not None:
                    self.latest_symbols_price_data[s].append(dat)
        if len(new_loaded) == 0:
            self._all_latest_loaded = True
    
    def _get_new_data(self, symbol):
        """
        Returns the latest bar from the data feed as a tuple of 
        (symbol, datetime, open, low, high, close, volume).
        """
        for idx,dat in self.symbols_price_data[symbol]:
            #yield tuple([symbol, datetime.datetime.strptime(b[0], '%Y-%m-%d %H:%M:%S'),b[1][0], b[1][1], b[1][2], b[1][3], b[1][4]])
            yield tuple([
                    symbol,pd.Timestamp(idx),
                                dat[0], dat[1], dat[2], dat[3], dat[4]
                        ])

    def _assets_price_data_to_bid_ask(self, bar_df):
        """
        Converts the DataFrame from daily OHLCV 'bars' into a DataFrame
        of open and closing price timestamps.
        Optionally adjusts the open/close prices for corporate actions
        using any provided 'Adjusted Close' column.
        Parameters
        ----------
        `pd.DataFrame`
            The daily 'bar' OHLCV DataFrame.
        Returns
        -------
        `pd.DataFrame`
            The individually-timestamped open/closing prices, optionally
            adjusted for corporate actions.
        """
        bar_df = bar_df.sort_index()
        if self.adjust_prices:
            if 'Adj Close' not in bar_df.columns:
                raise ValueError(
                    "Unable to locate Adjusted Close pricing column in CSV data file. "
                    "Prices cannot be adjusted. Exiting."
                                )
            
            # Restrict solely to the open/closing prices
            oc_df = bar_df.loc[:, ['open', 'close', 'Adj Close']]

            # Adjust opening prices
            oc_df['Adj Open'] = (oc_df['Adj Close'] / oc_df['close']) * oc_df['open']
            oc_df = oc_df.loc[:, ['Adj Open', 'Adj Close']]
            oc_df.columns = ['open', 'close']
        else:
            oc_df = bar_df.loc[:, ['open', 'close']]

        # Convert bars into separate rows for open/close prices
        # appropriately timestamped
        seq_oc_df = oc_df.T.unstack(level=0).reset_index()
        seq_oc_df.columns = ['datetime', 'market', 'price']
        seq_oc_df.loc[seq_oc_df['market'] == 'open', 'datetime'] += pd.Timedelta(hours=9, minutes=30)
        seq_oc_df.loc[seq_oc_df['market'] == 'close', 'datetime'] += pd.Timedelta(hours=16, minutes=00)

        # TODO: Unable to distinguish between Bid/Ask, implement later
        dp_df = seq_oc_df[['datetime', 'price']]
        dp_df['bid'] = dp_df['price']
        dp_df['ask'] = dp_df['price']
        dp_df = dp_df.loc[:, ['datetime', 'bid', 'ask']].fillna(method='ffill').set_index('datetime').sort_index()
        return dp_df

    def get_historical_price_df(self,
                                asset,
                                price = None,
                                start_dt = None,
                                end_dt = None,
                                multi_index_columns:bool = True,
    )->pd.DataFrame:
        if price is not None:
            price = [price]
        else:
            price = ['open','high', 'low', 'close','volume']
        assert set(price).issubset(set(['open','high', 'low', 'close','volume']))

        if asset not in self.csv_symbols:
            return pd.DataFrame(columns=['open','high', 'low','close','volume'])
        else:
            for dt in [start_dt, end_dt]:
                if dt is not None:
                    try:
                        pd.Timestamp(dt)
                    except:
                        raise Exception("Assigned start_dt/end_dt needs to be convertable to pd.Timestamp")
            if start_dt is not None and end_dt is not None:
                assert pd.Timestamp(end_dt) > pd.Timestamp(start_dt)
            df = self._csv_to_df(self._csv_file_from_symbol(asset))
            df = df.set_index(df.index.tz_localize(settings.TIMEZONE))
            df = df.reindex(
                            index=self.csv_combined_index,
                            method='pad'
                            )[price]
            if multi_index_columns:
                df.columns = [
                              np.array([asset for _ in range(df.shape[1])]),
                              np.array(df.columns)
                             ]
            if start_dt is not None:
                df = df[df.index >= pd.Timestamp(start_dt)]
            if end_dt is not None:
                df = df[df.index <= pd.Timestamp(end_dt)]
            return df


    #!--| MAIN FUNCS
    def get_assets_historical_price_dfs(self,
                                        assets,
                                        start_dt=None,
                                        end_dt=None,
                                        price= None,
                                        adjusted=False
    )->pd.DataFrame:
        price_df = pd.concat(
                    [
            df for df in [
                self.get_historical_price_df(asset, price, start_dt, end_dt) for asset in assets
                         ] if not df.empty
                    ],
                    axis=1
                        )
        if price_df.empty:
            return None
        else:
            return price_df
        

    @functools.lru_cache(maxsize=1024 * 1024)
    def get_bid(self, dt, asset):
        """
        Obtain the bid price of an asset at the provided timestamp.
        Parameters
        ----------
        dt : `pd.Timestamp`
            When to obtain the bid price for.
        asset : `str`
            The asset symbol to obtain the bid price for.
        Returns
        -------
        `float`
            The bid price.
        """
        bid_ask_df = self.asset_bid_ask_frames[asset]
        try:
            bid = bid_ask_df.iloc[bid_ask_df.index.get_loc(dt, method='pad')]['bid']
        except KeyError:  # Before start date
            return np.NaN
        return bid

    @functools.lru_cache(maxsize=1024 * 1024)
    def get_ask(self, dt, asset):
        """
        Obtain the ask price of an asset at the provided timestamp.
        Parameters
        ----------
        dt : `pd.Timestamp`
            When to obtain the ask price for.
        asset : `str`
            The asset symbol to obtain the ask price for.
        Returns
        -------
        `float`
            The ask price.
        """
        bid_ask_df = self.asset_bid_ask_frames[asset]
        try:
            ask = bid_ask_df.iloc[bid_ask_df.index.get_loc(dt, method='pad')]['ask']
        except KeyError:  # Before start date
            return np.NaN
        return ask

    def assetsDF(self):
        return pd.DataFrame({'symbol':pd.Series(data=self.csv_symbols)})
