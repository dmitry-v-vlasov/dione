from enum import Enum
from typing import Optional
import abc

from datetime import datetime
import pandas as pd

from dataclasses import dataclass

import yfinance as yf


class RemoteDataSourceName(Enum):
    YAHOO_FINANCE = 'Yahoo! Finance'

    @classmethod
    def from_str(cls, value: str):
        if value == cls.YAHOO_FINANCE.value:
            return cls.YAHOO_FINANCE
        else:
            raise ValueError(f"Unknown RemoteDataSourceName: {value}")

    @classmethod
    def has_value(cls, value: str):
        return value in cls._value2member_map_

    @classmethod
    def values(cls):
        return list(cls._value2member_map_.values())


class IDataAdapter(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def history_data(self, start: Optional[datetime]=None, end: Optional[datetime]=None, interval: str = '1d') -> pd.DataFrame:
        raise NotImplementedError

    @abc.abstractmethod
    def symbol(self):
        raise NotImplementedError

    @abc.abstractmethod
    def name(self):
        raise NotImplementedError

    @abc.abstractmethod
    def long_name(self):
        raise NotImplementedError

    @abc.abstractmethod
    def currency(self):
        raise NotImplementedError

    @abc.abstractmethod
    def exchange(self):
        raise NotImplementedError

    @abc.abstractmethod
    def market(self):
        raise NotImplementedError

    @abc.abstractmethod
    def timezone(self):
        raise NotImplementedError

    @abc.abstractmethod
    def info(self):
        raise NotImplementedError


class BasicLocalDataAdapter(IDataAdapter):
    def __init__(self, ticker_name, history_data_frame: pd.DataFrame):
        self.__ticker = ticker_name
        self.__history = history_data_frame

    def history_data(self, start: Optional[datetime]=None, end: Optional[datetime]=None, interval: str = '1d') -> pd.DataFrame:
        return self.__history

    @property
    def symbol(self):
        return self.__ticker

    @property
    def name(self):
        return self.__ticker

    @property
    def long_name(self):
        return self.__ticker

    @property
    def currency(self):
        return None

    @property
    def exchange(self):
        return None

    @property
    def market(self):
        return None

    @property
    def timezone(self):
        return None

    @property
    def info(self):
        return None


class YahooFinanceRemoteDataAdapter(IDataAdapter):
    def __init__(self, ticker_name):
        self.__ticker = yf.Ticker(ticker_name)

    def history_data(self, start: Optional[datetime]=None, end: Optional[datetime]=None, interval: str = '1d') -> pd.DataFrame:
        return self.__ticker.history(interval=interval, start=start, end=end, keepna=True)

    @property
    def symbol(self):
        return self.__ticker.info['symbol']

    @property
    def name(self):
        return self.__ticker.info['shortName']

    @property
    def long_name(self):
        return self.__ticker.info['longName']

    @property
    def currency(self):
        return self.__ticker.info['currency']

    @property
    def exchange(self):
        return self.__ticker.info['exchange']

    @property
    def market(self):
        return self.__ticker.info['market']

    @property
    def timezone(self):
        return self.__ticker.info['exchangeTimezoneName']

    @property
    def info(self):
        return self.__ticker.info


class IData(metaclass=abc.ABCMeta):
    pass


@dataclass
class AbstractData(IData):
    data: IDataAdapter


@dataclass
class LocalData(AbstractData):
    source_path: str

    @property
    def loaded_data(self) -> pd.DataFrame:
        return self.data.history_data()


@dataclass
class RemoteData(AbstractData):
    source: RemoteDataSourceName
    data: IDataAdapter
    loaded_data: pd.DataFrame


@dataclass
class ComplexData(IData):
    remote_data: RemoteData
    local_data: LocalData


@dataclass
class QuotedInstrumentData(IData):
    ticker: str
    name: str
    description: str
    local_data: LocalData
    remote_data: RemoteData


class DataLoadingStrategyName(Enum):
    KEEP_LOCAL_SAVE_REMOTE = 'keep_local_save_remote'
    KEEP_LOCAL_SAVE_REMOTE_LATEST = 'keep_local_save_remote_latest'
    LOAD_REMOTE_TO_LOCAL_AND_REMOTE_AS_LATEST = 'load_remote_to_local_and_remote_as_latest'

    @classmethod
    def from_str(cls, value: str):
        if value == cls.KEEP_LOCAL_SAVE_REMOTE.value:
            return cls.KEEP_LOCAL_SAVE_REMOTE
        elif value == cls.KEEP_LOCAL_SAVE_REMOTE_LATEST.value:
            return cls.KEEP_LOCAL_SAVE_REMOTE_LATEST
        elif value == cls.LOAD_REMOTE_TO_LOCAL_AND_REMOTE_AS_LATEST.value:
            return cls.LOAD_REMOTE_TO_LOCAL_AND_REMOTE_AS_LATEST
        else:
            raise ValueError(f"Unknown DataLoadingStrategyName: {value}")

    @classmethod
    def has_value(cls, value: str):
        return value in cls._value2member_map_

    @classmethod
    def values(cls):
        return list(cls._value2member_map_.values())