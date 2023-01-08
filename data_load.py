from typing import Optional
from enum import Enum
from datetime import datetime
from pprint import pformat
import os.path as path

import pandas as pd
import yfinance as yf

import config_model as cfgm
import data_load_model as model

class AbstractDataLoader:
    def __init__(self, quoted_instrument: cfgm.QuotedInstrument):
        self._quoted_instrument = quoted_instrument

    def load_data(self) -> model.IData:
        raise NotImplementedError("Abstract DataLoader has no implementation.")


class RemoteDataLoader(AbstractDataLoader):
    def __init__(self, quoted_instrument: cfgm.QuotedInstrument):
        super().__init__(quoted_instrument)

    def load_data(self) -> model.RemoteData:
        data_loading_config = self._quoted_instrument.data_loading
        remote_data_loading_config = data_loading_config.remote_data_loading

        ticker = self._quoted_instrument.ticker
        source = remote_data_loading_config.source_name
        if not model.RemoteDataSourceName.has_value(source):
            raise ValueError(f"Remote data source {source} is not supported. \
                Supported source names: {pformat(model.RemoteDataSourceName.values(), width=5)}")

        remote_data_source_name = model.RemoteDataSourceName.from_str(source)
        if remote_data_source_name == model.RemoteDataSourceName.YAHOO_FINANCE:
            remote_data_adapter = model.YahooFinanceRemoteDataAdapter(ticker_name=ticker)
            local_data_table = remote_data_adapter.history_data(
                start=remote_data_loading_config.time_range.begin_time,
                end=remote_data_loading_config.time_range.end_time
            )
            remote_data = model.RemoteData(
                remote_data_adapter,
                remote_data_source_name,
                local_data_table
            )
            return remote_data
        else:
            raise ValueError(f"Remote data source {source} is not supported.")

    def store_data(self, required_file_path: Optional[str]=None) -> model.RemoteData:
        data_loading_config = self._quoted_instrument.data_loading
        file_path = data_loading_config.remote_data_loading.file_name
        target_file_path = required_file_path if required_file_path else file_path

        data = self.load_data()
        data_table = data.loaded_data
        data_table.to_csv(target_file_path)
        return data


class LocalDataLoader(AbstractDataLoader):
    def __init__(self, quoted_instrument: cfgm.QuotedInstrument, remote_data_loader: Optional[RemoteDataLoader]=None):
        super().__init__(quoted_instrument)
        self.__remote_data_loader = remote_data_loader

    def load_data(self) -> model.LocalData:
        data_loading_config = self._quoted_instrument.data_loading
        ticker = self._quoted_instrument.ticker
        file_path = data_loading_config.local_data_loading.file_name

        data_frame = None
        if self.__remote_data_loader is None:
            if not path.isfile(file_path):
                raise FileNotFoundError(f"File {file_path} does not exist.")

            data_frame = pd.read_csv(
                file_path,
                index_col=data_loading_config.date_column,
                parse_dates=True
            )
        else:
            remote_data = self.__remote_data_loader.load_data()
            data_frame = remote_data.loaded_data

        data = model.BasicLocalDataAdapter(ticker, history_data_frame=data_frame)
        return model.LocalData(data, file_path)

    def store_data(self, required_file_path: Optional[str]=None) -> model.LocalData:
        data_loading_config = self._quoted_instrument.data_loading
        file_path = data_loading_config.local_data_loading.file_name
        target_file_path = required_file_path if required_file_path else file_path

        data = self.load_data()
        data_table = data.data.history_data()
        data_table.to_csv(target_file_path)
        return data


class StrategyBasedDataLoader(AbstractDataLoader):

    def __init__(self, quoted_instrument: cfgm.QuotedInstrument):
        super().__init__(quoted_instrument)

    def load_data(self) -> model.ComplexData:
        data_loading_config = self._quoted_instrument.data_loading

        remote_config = data_loading_config.remote_data_loading
        file_path: str = remote_config.file_name
        file_directory = path.dirname(file_path)
        file_name = path.basename(file_path)
        file_name_main, file_name_extension = path.splitext(file_name)

        # =================================================================
        strategy_name = self.__obtain_loading_strategy(data_loading_config)
        # =================================================================

        # =================================================================
        target_remote_file_path = None
        if model.DataLoadingStrategyName.KEEP_LOCAL_SAVE_REMOTE == strategy_name:
            current_datetime = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')            
            target_file_name = file_name_main + '_' + current_datetime + file_name_extension
            target_remote_file_path = path.join(file_directory, target_file_name)
        elif model.DataLoadingStrategyName.KEEP_LOCAL_SAVE_REMOTE_LATEST == strategy_name or \
            model.DataLoadingStrategyName.LOAD_REMOTE_TO_LOCAL_AND_REMOTE_AS_LATEST == strategy_name:
            target_file_name = file_name_main + '_latest' + file_name_extension
            target_remote_file_path = path.join(file_directory, target_file_name)
        else:
            raise ValueError(f"Data loading strategy {strategy_name} is not supported for remote data.")

        remote_data_loader = RemoteDataLoader(self._quoted_instrument)
        remote_data = remote_data_loader.store_data(required_file_path=target_remote_file_path)
        # =================================================================

        # =================================================================
        target_local_file_path = None
        local_data_loader = None
        if model.DataLoadingStrategyName.LOAD_REMOTE_TO_LOCAL_AND_REMOTE_AS_LATEST == strategy_name:
            local_data_loader = LocalDataLoader(self._quoted_instrument, remote_data_loader)
        else:
            raise ValueError(f"Data loading strategy {strategy_name} is not supported for local data.")

        local_data = local_data_loader.store_data(required_file_path=target_local_file_path)
        # =================================================================

        return model.ComplexData(remote_data, local_data)

    def __obtain_loading_strategy(self, data_loading_config) -> model.DataLoadingStrategyName:
        strategy_value = data_loading_config.data_loading_stategy
        if not model.DataLoadingStrategyName.has_value(strategy_value):
            raise ValueError(f"Strategy {strategy_value} is not supported. \
                Supported strategy names: {pformat(model.DataLoadingStrategyName.values(), width=5)}")
        strategy_name = model.DataLoadingStrategyName.from_str(strategy_value)                
        return strategy_name