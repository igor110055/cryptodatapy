# import libraries
import os
import logging
import keyring
from dataclasses import dataclass
from typing import Union


# sets data credentials
def set_credential(cred_key: str, cred_val: Union[str, int]) -> None:

    """
    Sets credential value for credential key e.g. api keys, api secrets, db name, ....

    Parameters
    ----------
    cred_key: str
        Credential key for which value is to be stored.
    cred_val: str
        Credential value to be stored.
    """

    if isinstance(cred_val, int):
        cred_val = str(cred_val)

    try:
        keyring.set_password(cred_key, os.getlogin(), cred_val)
    except:
        logging.warning('Credentials could not be set.')


# retrieves data credentials
def get_credential(cred_key: str) -> str:
    """
    Gets credentials for system, e.g. api keys, api secrets, db name, ....

    Parameters
    ----------
    cred_key: str
        System for which credential is to be retrieved.

    Returns
    -------
    cred: str
        Credential for specified system.
    """

    cred = None

    try:
        cred = keyring.get_password(cred_key, os.getlogin())
    except:
        logging.warning('Credentials could not be retrieved.')

    return cred


# data credentials class
@dataclass
class DataCredentials:
    """

    Stores important data credentials used by the CryptoDataPypelihe project for data extraction, storage, ...

    """

    # SQL db for structured data
    # postgresql db credentials
    postgresql_db_address: str = get_credential('postgresql_db_address')
    postgresql_db_port: str = get_credential('postgresql_db_port')
    postgresql_db_username: str = get_credential('postgresql_db_username')
    postgresql_db_password: str = get_credential('postgresql_db_password')
    postgresql_db_name: str = get_credential('postgresql_db_name')

    # NoSQL DB for tick/unstructured data
    # Arctic/mongodb credentials
    mongo_db_username: str = get_credential('mongodb_username')
    mongo_db_password: str = get_credential('mongodb_password')
    mongo_db_name: str = get_credential('mongodb_name')

    #  API KEYS
    binance_api_key: str = get_credential('binance_api_key')
    binance_api_secret: str = get_credential('binance_api_secret')
    cryptocompare_api_key: str = get_credential('cryptocompare_api_key')
    glassnode_api_key: str = get_credential('glassnode_api_key')
    tiingo_api_key: str = get_credential('tiingo_api_key')
    finnhub_api_key: str = get_credential('finnhub_api_key')
    fred_api_key: str = get_credential('fred_api_key')
    quandl_api_key: str = get_credential('quandl_api_key')

    # API Limits
    cryptocompare_api_limit: int = get_credential('cryptocompare_api_limit')

    # URLs for get requests
    cryptocompare_base_url: str = get_credential('cryptocompare_base_url')
    glassnode_base_url: str = get_credential('glassnode_base_url')
    tiingo_base_url: str = get_credential('tiingo_base_url')
