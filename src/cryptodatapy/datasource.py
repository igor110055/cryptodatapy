# import libraries
import pandas as pd
from abc import ABC, abstractmethod
from typing import Iterable, Dict, Union


class DataSource(ABC):
    """
    DataSource is an abstract base class (ABS) which provides a blueprint for the properties and methods of
    concrete data source subclasses.

    """

    def __init__(
            self,
            source_type: str,
            categories: Iterable[str],
            assets: Dict[str, Union[str, Iterable[str]]],
            indexes: Dict[str, Union[str, Iterable[str]]],
            markets: Dict[str, Union[str, Iterable[str]]],
            market_types: Dict[str, Union[str, Iterable[str]]],
            fields: Dict[str, Union[str, Iterable[str]]],
            frequencies: Iterable[str],
            exchanges: Dict[str, Union[str, Iterable[str]]],
            base_url: str,
            api_key: str,
            max_obs_per_call: int,
            rate_limit
    ):

        """

        Parameters
        ----------

        source_type: str, {'exchange', 'data_vendor', 'library'}
            The type of data source, e.g. direct from the exchange, from data vendor/aggregator,
            or open source library/package/API.

        max_obs_per_call: int
            Maximum number of observations returned for each data request.

        Returns
        -------

        """

        self._source_type = source_type
        self._categories = categories
        self._assets = assets
        self._indexes = indexes
        self._markets = markets
        self._market_types = market_types
        self._fields = fields
        self._frequencies = frequencies
        self._exchanges = exchanges
        self._base_url = base_url
        self._api_key = api_key
        self._max_obs_per_call = max_obs_per_call
        self._rate_limit = rate_limit

    @property
    def source_type(self):
        """
        Returns the type of data source.
        """
        return self._source_type

    @source_type.setter
    def source_type(self, source_type):
        """
        Sets the type of data source.
        """
        if source_type in ['exchange', 'data_vendor', 'library']:
            self._source_type = source_type
        else:
            raise Exception("Invalid source type. Source types include: 'exchange', 'data_vendor' and 'library'.")

    @property
    def categories(self):
        """
        Returns a list of avalailable categories for the data source.
        """
        return self._categories

    @categories.setter
    def categories(self, categories):
        """
        Sets a list of available categories for the data source.
        """
        cat = []
        # convert str to list
        if isinstance(categories, str):
            categories = [categories]
        # check if valid category
        for category in categories:
            if category in ['crypto', 'fx', 'rates', 'equities', 'commodities', 'credit', 'macro', 'alt']:
                cat.append(category)
            else:
                raise Exception(
                    f"{category} is an invalid category. Valid categories are: 'crypto', 'fx', 'rates', 'equities', "
                    f"'commodities', 'credit', 'macro', 'alt'.")
                # set categories
        self._categories = cat

    @property
    def assets(self):
        """
        Returns a list of available assets for the data source.
        """
        return self._assets

    @assets.setter
    def assets(self, assets):
        """
        Sets a list of available assets for the data source.
        """
        raise Exception('Assets cannot be set. Use the method get_assets() to retrieve available assets.')

    @abstractmethod
    def get_assets(self):
        """
        Gets list of available assets from data source.
        """
        # to be implemented by subclasses

    @property
    def indexes(self):
        """
        Returns a list of available indices for the data source.
        """
        return self._indexes

    @indexes.setter
    def indexes(self, assets):
        """
        Sets a list of available indexes for the data source.
        """
        raise Exception('Indexes cannot be set. Use the method get_indexes() to retrieve available indexes.')

    @abstractmethod
    def get_indexes(self):
        """
        Gets a list of available indexes from data source.
        """
        # to be implemented by subclasses

    @property
    def markets(self):
        """
        Returns a list of available markets for the data source.
        """
        return self._markets

    @markets.setter
    def markets(self, markets):
        """
        Sets a list of available markets for the data source.
        """
        raise Exception('Markets cannot be set. Use the method get_markets() to retrieve available markets.')

    @abstractmethod
    def get_markets(self):
        """
        Gets a list of available markets for the data source.
        """
        # to be implemented by subclasses

    @property
    def market_types(self):
        """
        Returns a list of available market types for the data source.
        """
        return self._market_types

    @market_types.setter
    def market_types(self, values):
        """
        Sets a list of available market types for the data source.
        """
        mkt_types = []
        # convert str to list
        if isinstance(values, str):
            values = [values]
        # check if valid market types
        for value in values:
            if value in ['spot', 'perpetual_futures', 'futures', 'options']:
                mkt_types.append(value)
            else:
                raise Exception(f"{value} is an invalid market type. Valid market types are: 'spot', 'futures', "
                                f"'perpetual_futures', 'options'")
                # set categories
        self._market_types = mkt_types

    @property
    def fields(self):
        """
        Returns a list of available fields for the data source.
        """
        return self._fields

    @fields.setter
    def fields(self, fields):
        """
        Sets a list of available fields for the data source.
        """
        raise Exception('Fields cannot be set. Use the method get_fields() to retrieve available fields.')

    @abstractmethod
    def get_fields(self):
        """
        Gets a list of available fields for the data source.
        """
        # to be implemented by subclasses

    @property
    def frequencies(self):
        """
        Returns a list of available data frequencies for the data source.
        """
        return self._frequencies

    @frequencies.setter
    def frequencies(self, frequencies):
        """
        Sets a list of available data frequencies for the data source.
        """
        freq = []
        # convert str to list
        if isinstance(frequencies, str):
            frequencies = [frequencies]
        # check if valid data frequency
        for frequency in frequencies:
            if frequency in ['tick', '1min', '5min', '10min', '20min', '30min', '1h', '2h', '4h', '8h', 'd', 'w', 'm']:
                freq.append(frequency)
            else:
                raise Exception(f"{frequency} is an invalid data frequency. Valid frequencies are: 'tick', '1min', "
                                f"'5min', '10min', '15min', '30min', '1h', '2h', '4h', '8h', 'd', 'w', 'm'.")
                # set categories
        self._frequencies = freq

    @property
    def exchanges(self):
        """
        Returns a list of available exchanges for the data source.
        """
        return self._exchanges

    @exchanges.setter
    def exchanges(self, exchanges):
        """
        Sets a list of available exchanges for the data source.
        """
        raise Exception('Exchanges cannot be set. Use the method get_exchanges() to retrieve available exchanges.')

    @abstractmethod
    def get_exchanges(self):
        """
        Gets a list of available exchanges for the data source.
        """
        # to be implemented by subclasses

    @property
    def base_url(self):
        """
        Returns the base url for the data source.
        """
        return self._base_url

    @base_url.setter
    def base_url(self, url):
        """
        Sets the base url for the data source.
        """
        if not isinstance(url, str):
            raise Exception('Base url must be a string containing the data source web address.')
        else:
            self._base_url = url

    @property
    def api_key(self):
        """
        Returns the api key for the data source.
        """
        return self._api_key

    @api_key.setter
    def api_key(self, api_key):
        """
        Sets the api key for the data source.
        """
        if not isinstance(api_key, str):
            raise Exception('Api key must be a string.')
        else:
            self._api_key = api_key

    @property
    def max_obs_per_call(self):
        """
        Returns the maximum observations per API call for the data source.
        """
        return self._max_obs_per_call

    @max_obs_per_call.setter
    def max_obs_per_call(self, limit):
        """
        Sets the maximum number of observations per API call for the data source.
        """
        if not isinstance(limit, int):
            raise Exception('Maximum number of observations per API call must be an integer.')
        else:
            self._max_obs_per_call = limit

    @property
    def rate_limit(self):
        """
        Returns the number of API calls made and remaining.
        """
        return self._rate_limit

    @rate_limit.setter
    def rate_limit(self, limit):
        """
        Sets the number of API calls made and left.
        """
        raise Exception('Rate limit cannot be set. Use the method get_rate_limit() to retrieve API call info.')

    @abstractmethod
    def get_rate_limit(self):
        """
        Gets the number of API calls made and remaining.
        """
        # to be implemented by subclasses

    @abstractmethod
    def convert_data_req_params(self, data_req: Dict[str, Union[str, int, list]]) -> dict:
        """
        Converts AlphaFactory data request parameters to data source parameters.
        """
        # to be implemented by subclasses

    @abstractmethod
    def fetch_data(self, data_req: Dict[str, Union[str, int, list]]) -> pd.DataFrame:
        """
        Submits data request to API.
        """
        # to be implemented by subclasses