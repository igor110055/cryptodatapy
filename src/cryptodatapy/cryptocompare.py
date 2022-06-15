# import libraries
import pandas as pd
import logging
import sys
import requests
from datetime import datetime, timedelta
from time import sleep
from typing import Iterable, Optional, Dict, Type
sys.path.append('../')
from util.datacredentials import *
from util.convertparams import *
from datarequest import *
from datasource import *


# data credentials
data_cred = DataCredentials()


class CryptoCompare(DataSource):
    """
    Retrieves data from CryptoCompare API.
    """

    def __init__(
            self,
            source_type: str = 'data_vendor',
            categories: Iterable[str] = ['crypto'],
            assets: Dict[str, Union[str, Iterable[str]]] = None,
            indexes: Dict[str, Union[str, Iterable[str]]] = None,
            markets: Dict[str, Union[str, Iterable[str]]] = None,
            market_types: Dict[str, Union[str, Iterable[str]]] = ['spot'],
            fields: Dict[str, Union[str, Iterable[str]]] = None,
            frequencies: Iterable[str] = ['min', 'h', 'd'],
            exchanges: Dict[str, Union[str, Iterable[str]]] = None,
            base_url: str = data_cred.cryptocompare_base_url,
            api_key: str = data_cred.cryptocompare_api_key,
            max_obs_per_call: int = 2000,
            rate_limit=None
    ):
        DataSource.__init__(self, source_type, categories, assets, indexes, markets, market_types, fields, frequencies,
                            exchanges, base_url, api_key, max_obs_per_call, rate_limit)

        # api key
        if api_key is None:
            raise Exception(f"Set your api key. Alternatively, you can use the function "
                            f"{set_credential.__name__} which uses keyring to store your "
                            f"api key in {DataCredentials.__name__}.")
        # set assets
        if assets is None:
            self._assets = self.get_assets(as_list=True)
        # set indexes
        if indexes is None:
            self._indexes = self.get_indexes(as_list=True)
        # set markets
        if markets is None:
            self._markets = self.get_markets()
        # set fields
        if fields is None:
            self._fields = self.get_fields()
        # set exchanges
        if exchanges is None:
            self._exchanges = self.get_exchanges(as_list=True)
        # set rate limit
        if rate_limit is None:
            self._rate_limit = self.get_rate_limit()

    def get_assets(self, as_list=False) -> Union[pd.DataFrame, list]:
        """
        Gets list of available assets from data source.

        Parameters
        ----------
        as_list: bool, default False
            Returns available assets as list.

        Returns
        -------
        assets: DataFrame or list
            Info on available assets for data source.
        """
        try:  # try get request
            url = data_cred.cryptocompare_base_url + 'all/coinlist'
            params = {
                'api_key': self.api_key
            }
            r = requests.get(url, params=params)
            assert r.json()['Response'] == 'Success'

        except AssertionError as e:
            logging.warning(f"Failed to get asset info.")

        # format response
        assets = pd.DataFrame(r.json()['Data']).T
        # asset list
        if as_list:
            assets = list(assets.index)

        return assets

    # get index info, or list
    def get_indexes(self, as_list=False) -> Union[pd.DataFrame, list]:

        try:  # try get request
            url = data_cred.cryptocompare_base_url + 'index/list'
            params = {
                'api_key': self.api_key
            }
            r = requests.get(url, params=params)
            assert r.json()['Response'] == 'Success'

        except AssertionError as e:
            logging.warning(f"Failed to get index info.")

        # format response
        indexes = pd.DataFrame(r.json()['Data']).T
        # asset list
        if as_list:
            indexes = list(indexes.index)

        return indexes

    # get markets info, or list
    def get_markets(self) -> Dict[str, list]:

        try:  # try get request
            url = data_cred.cryptocompare_base_url + 'v2/cccagg/pairs'
            params = {
                'api_key': self.api_key
            }
            r = requests.get(url, params=params)
            assert r.json()['Response'] == 'Success'

        except AssertionError as e:
            logging.warning(f"Failed to get index info.")

        # format response
        data_resp = r.json()['Data']
        mkts_dict = {}
        for asset in data_resp['pairs']:
            mkts_dict[asset] = data_resp['pairs'][asset]['tsyms']

        return mkts_dict

    def get_fields(self) -> Iterable[str]:

        ohlcv_fields = ['open', 'high', 'low', 'close', 'volume']

        try:  # try get request for on-chain data
            url = data_cred.cryptocompare_base_url + 'blockchain/latest?fsym=BTC'
            params = {
                'api_key': self.api_key
            }
            r = requests.get(url, params=params)
            assert r.json()['Response'] == 'Success'

        except AssertionError as e:
            logging.warning(f"Failed to get on-chain data info.")

        # format response
        data_resp = r.json()['Data']
        fields_list = ohlcv_fields + (list(data_resp.keys()))[4:]

        return fields_list

    def get_exchanges(self, as_list=False) -> Union[pd.DataFrame, list]:

        try:  # try get request
            url = data_cred.cryptocompare_base_url + 'exchanges/general'
            params = {
                'api_key': self.api_key
            }
            r = requests.get(url, params=params)
            assert r.json()['Response'] == 'Success'

        except AssertionError as e:
            logging.warning(f"Failed to get exchanges info.")

        # format response
        exch = pd.DataFrame(r.json()['Data']).T
        exch.set_index('Name', inplace=True)
        # asset list
        if as_list:
            exch = list(exch.index)

        return exch

    def get_rate_limit(self) -> pd.DataFrame:

        try:  # try get request
            url = 'https://min-api.cryptocompare.com/stats/rate/limit'
            params = {
                'api_key': self.api_key
            }
            r = requests.get(url, params=params)
            assert r.json()['Response'] == 'Success'

        except AssertionError as e:
            logging.warning(f"Failed to get rate limit info.")

        # format response
        rate_limit = pd.DataFrame(r.json()['Data'])

        return rate_limit

    def convert_data_req_params(self, data_req: Type[DataRequest]) -> Dict[str, Union[str, int, list]]:
        """
        Converts CryptoDataPypeline data request parameters to CryptoCompare API format.
        """

        # convert to list if str
        if isinstance(data_req.tickers, str):
            tickers = [data_req.tickers]
        else:
            tickers = data_req.tickers
        # convert tickers to uppercase and add to dict
        tickers = [ticker.upper() for ticker in tickers]

        # convert freq
        if data_req.freq[-3:] == 'min':
            freq = 'histominute'
        elif data_req.freq[-1] == 'h':
            freq = 'histohour'
        elif data_req.freq == 'd':
            freq = 'histoday'
        else:
            freq = 'histoday'

        # convert quote ccy
        # no quote
        if data_req.quote_ccy is None:
            quote_ccy = 'USD'
        else:
            quote_ccy = data_req.quote_ccy
            # quote ccy to uppercase and add to dict
        quote_ccy = quote_ccy.upper()

        # convert exch
        # no exch
        if data_req.exch is None:
            exch = 'CCCAGG'
        else:
            exch = data_req.exch

        # convert date format
        # min freq, Cryptocompare freemium will only return the past week of min OHLCV data
        if data_req.freq[-3:] == 'min':
            start_date = round((datetime.now() - timedelta(days=7)).timestamp())
        # no start date
        if data_req.start_date is None:
            start_date = convert_datetime_to_unix_tmsp('2010-01-01')
        else:
            start_date = convert_datetime_to_unix_tmsp(data_req.start_date)
        # end date
        if data_req.end_date is None:
            end_date = convert_datetime_to_unix_tmsp(datetime.utcnow())
        else:
            end_date = convert_datetime_to_unix_tmsp(data_req.end_date)

        # add to params dict
        cc_data_req = {'tickers': tickers, 'frequency': freq, 'currency': quote_ccy, 'exchange': exch,
                       'start_date': start_date, 'end_date': end_date, 'fields': data_req.fields,
                       'trials': data_req.trials, 'pause': data_req.pause}

        return cc_data_req

    # wrangle OHLCV data resp
    def wrangle_ohlcv_resp(self, data_req: Type[DataRequest], data_resp: pd.DataFrame) -> pd.DataFrame:
        """
        Wrangles OHLCV data response.
        """

        # make copy of df
        df = data_resp.copy()

        # create date and convert to datetime
        if 'date' not in df.columns and 'time' in df.columns:
            df['date'] = pd.to_datetime(df['time'], unit='s')

        # set datetimeindex and sort index by date
        df = df.set_index('date').sort_index()

        # rename volume col
        if 'volume' not in df.columns and 'volumefrom' in df.columns:
            df.rename(columns={'volumefrom': 'volume'}, inplace=True)

        # keep only ohlcv cols
        df = df.loc[:, ['open', 'high', 'low', 'close', 'volume']]

        # remove duplicate indexes/rows
        df = df[~df.index.duplicated()]

        # remove rows where close is 0
        df = df[df['close'] != 0].dropna()

        # filter for desired start to end date
        if data_req.start_date is not None:
            df = df[(df.index >= data_req.start_date)]
        if data_req.end_date is not None:
            df = df[(df.index <= data_req.end_date)]

        return df

    # wrangle onchain df
    def wrangle_onchain_resp(self, data_req: Type[DataRequest], data_resp: pd.DataFrame) -> pd.DataFrame:
        """
        Wrangles on-chain data response.
        """

        # make copy of df
        df = data_resp.copy()

        # create date and convert to datetime
        if 'date' not in df.columns and 'time' in df.columns:
            df['date'] = pd.to_datetime(df['time'], unit='s')
            df.drop(columns=['time'], inplace=True)

        # set datetimeindex and sort index by date
        df = df.set_index('date').sort_index()

        # rename symbol col
        if 'symbol' in df.columns:
            df.rename(columns={'symbol': 'ticker'}, inplace=True)

        # drop id
        df = df.drop(columns=['id'])

        # remove duplicate indexes/rows
        df = df[~df.index.duplicated()]

        # remove rows where close is 0
        # df = df[df != 0].dropna()

        # filter for desired start to end date
        if data_req.start_date is not None:
            df = df[(df.index >= data_req.start_date)]
        if data_req.end_date is not None:
            df = df[(df.index <= data_req.end_date)]

        # reset index
        df = df.reset_index().set_index(['date', 'ticker']).sort_index()

        # convert to float
        df = df.astype(float)

        return df

    def fetch_ohlcv(self, data_req: Type[DataRequest]) -> pd.DataFrame:
        """
        Submit data request to API for OHLCV data.
        """

        # convert data request parameters to CryptoCompare format
        cc_data_req = self.convert_data_req_params(data_req)
        # empty df to add data
        df = pd.DataFrame()

        # loop through tickers
        for ticker in cc_data_req['tickers']:

            # start and end date
            end_date = cc_data_req['end_date']
            # create empty ohlc df
            df0 = pd.DataFrame()
            # set number of attempts and bool for while loop
            attempts = 0
            # run a while loop to pull ohlcv prices in case the attempt fails
            while attempts < cc_data_req['trials']:
                try:  # fetch OHLCV data
                    # get request
                    url = self.base_url + f"v2/{cc_data_req['frequency']}"
                    params = {
                        'fsym': ticker,
                        'tsym': cc_data_req['currency'],
                        'limit': self.max_obs_per_call,
                        'e': cc_data_req['exchange'],
                        'toTs': end_date,
                        'api_key': self.api_key
                    }
                    r = requests.get(url, params=params)
                    # resp message
                    assert r.json()['Response'] == 'Success'
                    data = pd.DataFrame(r.json()['Data']['Data'])
                    # add data to empty df
                    df0 = pd.concat([df0, data])
                    # check if all data has been extracted
                    if len(data) < (self.max_obs_per_call + 1) or data.close[0] == 0 or data.close[0].astype(
                            str) == 'nan':
                        break
                    # reset end date and pause before calling API
                    else:
                        # change end date
                        end_date = data.time[0]
                        sleep(cc_data_req['pause'])

                except AssertionError as e:
                    attempts += 1
                    sleep(cc_data_req['pause'])
                    logging.warning(f"Failed to pull data for {ticker} after attempt #{str(attempts)}.")
                    if attempts == 3:
                        logging.warning(
                            f"Failed to pull data from Cryptocompare for {ticker} after many attempts due to following error: {str(r.json()['Message'])}.")
                        break

                except Exception as e:
                    logging.warning(e)
                    logging.warning(
                        "The data's response format has most likely changed.\n Review Cryptocompares response format and make changes to AlphaFactory's code base.")
                    break

            # wrangle data resp
            if not df0.empty:
                df1 = self.wrangle_ohlcv_resp(data_req, df0)
                # add ticker to df0 and reset index
                if len(data_req.tickers) > 1:
                    df1['ticker'] = ticker
                    df1 = df1.reset_index().set_index(['date', 'ticker']).sort_index()
                # concat df and df1
                df = pd.concat([df, df1])

        return df

    def fetch_onchain(self, data_req: Type[DataRequest]) -> pd.DataFrame:
        """
        Submit data request to API for on-chain data.
        """

        # convert data request parameters to CryptoCompare format
        cc_data_req = self.convert_data_req_params(data_req)
        # empty df to add data
        df = pd.DataFrame()

        # check if frequency daily
        if cc_data_req['frequency'] != 'histoday':
            raise Exception(f"On-chain data is only available on a daily frequency."
                            f"Change data request frequency to 'd' and try again.")

        # check if field available
        onchain_list = self.get_fields()[5:]
        if not any(i in onchain_list for i in cc_data_req['fields']):
            raise Exception(f"Field is not an on-chain data field. Check available fields with "
                            f"get_fields() method and try again.")

        # loop through tickers
        for ticker in cc_data_req['tickers']:

            # start and end date
            end_date = cc_data_req['end_date']
            # create empty ohlc df
            df0 = pd.DataFrame()
            # set number of attempts and bool for while loop
            attempts = 0
            # run a while loop to pull on-chain data in case the attempt fails
            while attempts < cc_data_req['trials']:
                try:

                    # get request
                    url = self.base_url + f"blockchain/histo/day?"
                    params = {
                        'fsym': ticker,
                        'limit': self.max_obs_per_call,
                        'toTs': end_date,
                        'api_key': self.api_key
                    }
                    r = requests.get(url, params=params)
                    # resp message
                    assert r.json()['Response'] == 'Success'
                    data = pd.DataFrame(r.json()['Data']['Data'])
                    # add data to empty df
                    df0 = pd.concat([df0, data])
                    # check if all data has been extracted
                    if len(data) < (self.max_obs_per_call + 1) or all(data.iloc[0] == 0) or all(
                            data.iloc[0].astype(str) == 'nan'):
                        break
                    # reset end date and pause before calling API
                    else:
                        # change end date
                        end_date = data.time[0]
                        sleep(cc_data_req['pause'])

                except AssertionError as e:
                    attempts += 1
                    sleep(cc_data_req['pause'])
                    logging.warning(f"Failed to pull data for {ticker} after attempt #{str(attempts)}.")
                    if attempts == 3:
                        logging.warning(
                            f"Failed to pull data from Cryptocompare for {ticker} after many attempts due to following error: {str(r.json()['Message'])}.")
                        break

                except Exception as e:
                    logging.warning(e)
                    logging.warning(
                        "The data's response format has most likely changed.\n Review Cryptocompares response format and make changes to AlphaFactory's code base.")
                    break

            # wrangle data resp
            if not df0.empty:
                df1 = self.wrangle_onchain_resp(data_req, df0)
                # concat df and df1
                df = pd.concat([df, df1])

        return df

    def fetch_data(self, data_req: Type[DataRequest]) -> pd.DataFrame:
        """
        Fetches either OHLCV or on-chain data.

        Parameters
        ----------
        data_req: dict
            Data request parameters.

        Returns
        -------
        df: DataFrame
            DataFrame with DatetimeIndex (level 0) and ticker (level 1) index, and OHLCV or on-chain data (cols)
        """

        # convert data request parameters to CryptoCompare format
        cc_data_req = self.convert_data_req_params(data_req)

        # check if fields available
        fields_list = self.get_fields()
        if not all(i in fields_list for i in cc_data_req['fields']):
            raise Exception('Fields are not available. Check available fields with get_fields() method and try again.')

        # fields lists
        ohlcv_list = fields_list[:5]
        onchain_list = fields_list[5:]

        # store in empty df
        df = pd.DataFrame()

        # fetch OHLCV data
        if any(i in ohlcv_list for i in cc_data_req['fields']):
            try:
                df0 = self.fetch_ohlcv(data_req)
                df = pd.concat([df, df0], axis=1)

            except Exception as e:
                logging.warning(e)

        # fetch on-chain data
        if any(i in onchain_list for i in cc_data_req['fields']):
            try:
                df1 = self.fetch_onchain(data_req)
                df = pd.concat([df, df1], axis=1)

            except Exception as e:
                logging.warning(e)

        # check if df empty
        if df.empty:
            raise Exception('Check data request parameters and try again.')

        # filter df for desired fields and sort index by date
        df = df.loc[:, cc_data_req['fields']]
        if len(data_req.tickers) > 1:
            df = df.sort_index()

        return df.sort_index()