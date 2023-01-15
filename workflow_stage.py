import typing
import warnings
from typing import Any
from collections import OrderedDict
from pathlib import Path
from datetime import datetime
import numpy as np

import pandas as pd

# import dataprep.eda as autoeda
import sweetviz as eda_sv
import pandas_profiling as eda_pp

import strings as ustr
import collections_iterables as colit
import file_system as fs
import config_logging as clog
import config_model as cfgm
import data_load as dl
import data_load_model as dlm
import templates
import workflow as wf


class DataLoadCommand(wf.AbstractCommand):

    LOGGER = clog.get_logger('DataLoadCommand')

    def __init__(self, use_remote_data: bool=True):
        super().__init__()
        self.__use_remote_data = use_remote_data

    def execute(self, context: dict):
        config: cfgm.Config = context['config']
        data = OrderedDict[str, dlm.ComplexData]()

        quoted_instrument = config.research.target_quoted_instrument
        self.__load_quoted_instrument(data, quoted_instrument)

        for quoted_instrument in config.research.quoted_instruments:
            self.__load_quoted_instrument(data, quoted_instrument)

        context['data'] = data
        return wf.CommandState.SUCCESS

    def __load_quoted_instrument(self, data: OrderedDict[str, dlm.ComplexData], quoted_instrument: cfgm.QuotedInstrument):
        quoted_instrument_data_loader = dl.StrategyBasedDataLoader(quoted_instrument)
        quoted_instrument_data = quoted_instrument_data_loader.load_data()
        data[quoted_instrument.ticker] = quoted_instrument_data
        if self.__use_remote_data:
            self.LOGGER.info(f"Loaded data for the quoted instrument '{quoted_instrument.ticker}.' \
                    DataFrame shape: {quoted_instrument_data.remote_data.loaded_data.shape}")
        else:
            self.LOGGER.info(f"Loaded data for the quoted instrument '{quoted_instrument.ticker}.' \
                    DataFrame shape: {quoted_instrument_data.local_data.loaded_data.shape}")



class DataTendingCommand(wf.AbstractCommand):

    LOGGER = clog.get_logger('DataTendingCommand')

    def __init__(self):
        super().__init__()

    def execute(self, context: dict):
        config: cfgm.Config = context['config']
        data: OrderedDict[str, dlm.ComplexData] = context['data']
        
        quoted_instrument = config.research.target_quoted_instrument
        self.__tend_quoted_instrument_data(data[quoted_instrument.ticker], quoted_instrument)

        for quoted_instrument in config.research.quoted_instruments:
            self.__tend_quoted_instrument_data(data[quoted_instrument.ticker], quoted_instrument)

        return wf.CommandState.SUCCESS

    def __tend_quoted_instrument_data(self, data: dlm.ComplexData, quoted_instrument: cfgm.QuotedInstrument):
        self.LOGGER.info(f"Tending quoted instrument '{quoted_instrument.ticker}'...")
        tending_config = quoted_instrument.data_transformation.tending
        remote_dataframe = data.remote_data.loaded_data
        local_dataframe = data.local_data.loaded_data
        if 'index' in tending_config:
            if 'reset' in tending_config['index']:
                reset_changes = tending_config['index']['reset']
                for reset_change in reset_changes:
                    if reset_change == 'localize':
                        if not remote_dataframe.empty and isinstance(remote_dataframe.index, pd.DatetimeIndex):
                            remote_dataframe.index = remote_dataframe.index.tz_localize(None)
                            self.LOGGER.info(f"Localize datetime index of {quoted_instrument.ticker} remote data DataFrame")
                        if not local_dataframe.empty and isinstance(local_dataframe.index, pd.DatetimeIndex):
                            local_dataframe.index = local_dataframe.index.tz_localize(None)
                            self.LOGGER.info(f"Localize datetime index of {quoted_instrument.ticker} local data DataFrame")

        if 'columns' in tending_config:
            if 'remove' in tending_config['columns']:
                columns = tending_config['columns']['remove']
                if columns:
                    if not remote_dataframe.empty:
                        remote_dataframe.drop(columns=columns, inplace=True)
                    if not local_dataframe.empty:
                        local_dataframe.drop(columns=columns, inplace=True)

            if 'change_rules' in tending_config['columns']:
                change_rules = tending_config['columns']['change_rules']
                if isinstance(change_rules, dict):
                    for column_name, change_rule in change_rules.items():
                        if change_rule == 'float':
                            if not remote_dataframe.empty:
                                remote_dataframe[column_name] = remote_dataframe[column_name].astype(float)
                            if not local_dataframe.empty:
                                local_dataframe[column_name] = local_dataframe[column_name].astype(float)
 
            if 'names' in tending_config['columns']:
                if tending_config['columns']['names']['to_snake_case']:
                    if not remote_dataframe.empty:
                        remote_dataframe.rename(
                            columns={column_name: ustr.to_snake_case(column_name) for column_name in remote_dataframe.columns.to_list()},
                            inplace=True
                        )
                    if not local_dataframe.empty:
                        local_dataframe.rename(
                            columns={column_name: ustr.to_snake_case(column_name) for column_name in local_dataframe.columns.to_list()},
                            inplace=True
                        )

        self.LOGGER.info(f"Tended quoted instrument '{quoted_instrument.ticker}': \
            Column names: remote - {remote_dataframe.columns}, local - {local_dataframe.columns}")
        

EDA_DIRECTORY: Path = Path('./data/generated/eda')
EDA_DIRECTORY.mkdir(parents=True, exist_ok=True)


class AutoEdaCommand(wf.AbstractCommand):
    
    LOGGER = clog.get_logger('AutoEdaCommand')

    EDA_FILE_NAME: str = 'eda.html'
    
    def __init__(self,
                 context_data_name: str='data',
                 use_remote_data: bool=True,
                 eda_name: str='default',
                 report_joined: bool=False):
        super().__init__()
        self.__context_data_name = context_data_name
        self.__use_remote_data = use_remote_data
        self.__eda_name = eda_name
        self.__report_joined = report_joined

    def execute(self, context: dict):
        config: cfgm.Config = context['config']
        research: cfgm.Research = config.research
        data: dict[str, Any] = context[self.__context_data_name]
        
        if not data:
            raise ValueError(f"Empty data in the workflow context section '{self.__context_data_name}'.")

        if isinstance(list(data.values())[0], dlm.ComplexData):
            self.LOGGER.info("Making EDA for raw dlm.ComplexData")
            raw_data: dict[str, dlm.ComplexData] = data

            target_instrument_data = raw_data[research.target_quoted_instrument.ticker]
            data_for_eda = target_instrument_data.remote_data.loaded_data \
                if self.__use_remote_data \
                    else target_instrument_data.local_data.loaded_data

            quoted_instrument = config.research.target_quoted_instrument
            self.__quoted_instrument_data_autoeda(data_for_eda, quoted_instrument.ticker)
            
            for quoted_instrument in config.research.quoted_instruments:
                instrument_data = raw_data[quoted_instrument.ticker]
                data_for_eda = instrument_data.remote_data.loaded_data \
                    if self.__use_remote_data \
                        else instrument_data.local_data.loaded_data
                self.__quoted_instrument_data_autoeda(data_for_eda, quoted_instrument.ticker)

            if self.__report_joined:
                data_for_eda_dict = {
                    ticker: data_object.remote_data.loaded_data \
                        if self.__use_remote_data \
                            else data_object.local_data.loaded_data for ticker, data_object in raw_data.items()
                }
                self.__report_joined_eda(
                    data_for_eda_dict,
                    config.research.target_quoted_instrument,
                    config.research.quoted_instruments
                )
        elif isinstance(list(data.values())[0], pd.DataFrame):
            self.LOGGER.info("Making EDA for pd.DataFrame(s)")
            pd_data: dict[str, pd.DataFrame] = data

            target_instrument_data = pd_data[research.target_quoted_instrument.ticker]
            data_for_eda = target_instrument_data
            quoted_instrument = config.research.target_quoted_instrument
            self.__quoted_instrument_data_autoeda(data_for_eda, quoted_instrument.ticker)
            
            for quoted_instrument in config.research.quoted_instruments:
                instrument_data = pd_data[quoted_instrument.ticker]
                data_for_eda = instrument_data
                self.__quoted_instrument_data_autoeda(data_for_eda, quoted_instrument.ticker)

            if self.__report_joined:
                data_for_eda_dict = {
                    ticker: data_object for ticker, data_object in pd_data.items()
                }
                self.__report_joined_eda(
                    data_for_eda_dict,
                    config.research.target_quoted_instrument,
                    config.research.quoted_instruments
                )
        else:
            raise ValueError(f"Unsupported data type in the workflow context section '{self.__context_data_name}'")

        return wf.CommandState.SUCCESS

    def __quoted_instrument_data_autoeda(self, data_for_eda: pd.DataFrame, quoted_instrument_ticker: str):
        eda_directory = Path(EDA_DIRECTORY, self.__eda_name, quoted_instrument_ticker)
        eda_directory.mkdir(parents=True, exist_ok=True)
        self.LOGGER.info(f"[{quoted_instrument_ticker}] Making EDA report ['{self.__eda_name}'], \
shape = {data_for_eda.shape} in the directory {str(eda_directory)}...")

        with warnings.catch_warnings():
            warnings.simplefilter(action='ignore', category=FutureWarning)
            eda_report_sv: eda_sv.DataframeReport = eda_sv.analyze(data_for_eda)
            Path(eda_directory, 'sweet-vis').mkdir(parents=True, exist_ok=True)
            eda_path_sv = Path(eda_directory, 'sweet-vis', self.EDA_FILE_NAME)
            eda_report_sv.show_html(str(eda_path_sv), open_browser=False)
        
        eda_report_pp: eda_pp.ProfileReport = eda_pp.ProfileReport(data_for_eda, tsmode=True)
        Path(eda_directory, 'pandas-profiling').mkdir(parents=True, exist_ok=True)
        eda_path_pp = Path(eda_directory, 'pandas-profiling', self.EDA_FILE_NAME)
        eda_report_pp.to_file(str(eda_path_pp))


    def __report_joined_eda(self, data_dictionary: dict[str, pd.DataFrame],
                            target_quoted_instrument: cfgm.QuotedInstrument,
                            quoted_instruments: list[cfgm.QuotedInstrument]):
        eda_directory = Path(EDA_DIRECTORY, self.__eda_name, 'joined_report')
        eda_directory.mkdir(parents=True, exist_ok=True)
        self.LOGGER.info(f"Making joined EDA report ['{self.__eda_name}'] for all tickers...")

        with warnings.catch_warnings():
            warnings.simplefilter(action='ignore', category=FutureWarning)
            joined_data: pd.DataFrame = data_dictionary[target_quoted_instrument.ticker].copy(deep=True)
            for quoted_instrument in quoted_instruments:
                right_data = data_dictionary[quoted_instrument.ticker]
                right_suffix = '_' + quoted_instrument.ticker
                right_data = right_data.add_suffix(right_suffix)
                self.LOGGER.info(f"[{quoted_instrument.ticker}] Right joining... right_suffix = '{right_suffix}'")
                joined_data = joined_data.join(
                    right_data,
                    how='outer'
                )
            
            self.LOGGER.info(f"Joined report column_types:\n{joined_data.info()}")

            eda_report_sv: eda_sv.DataframeReport = eda_sv.analyze(joined_data)
            Path(eda_directory, 'sweet-vis').mkdir(parents=True, exist_ok=True)
            eda_path_sv = Path(eda_directory, 'sweet-vis', self.EDA_FILE_NAME)
            eda_report_sv.show_html(str(eda_path_sv), open_browser=False)
        
            eda_report_pp: eda_pp.ProfileReport = eda_pp.ProfileReport(joined_data, tsmode=True)
            Path(eda_directory, 'pandas-profiling').mkdir(parents=True, exist_ok=True)
            eda_path_pp = Path(eda_directory, 'pandas-profiling', self.EDA_FILE_NAME)
            eda_report_pp.to_file(str(eda_path_pp))


class CheckDatesCommand(wf.AbstractCommand):

    LOGGER = clog.get_logger('CheckDatesCommand')

    def __init__(self, use_remote_data: bool=True):
        super().__init__()
        self.__use_remote_data = use_remote_data

    def execute(self, context: dict):
        config: cfgm.Config = context['config']
        time_range = config.research.machine_learning.time_range
        begin_time = time_range.begin_time
        end_time = time_range.end_time
        split_time = config.research.machine_learning.split_time

        if begin_time >= end_time:
            raise ValueError(f"End time in the target time range must be in the future, not is the past: '{time_range}'")
        if not (begin_time < split_time < end_time):
            raise ValueError(f"Split time must be in the range [{begin_time}, {end_time}] but is {split_time}")

        data: OrderedDict[str, dlm.ComplexData] = context['data']
        quoted_instrument = config.research.target_quoted_instrument
        self.__check_quoted_instrument_dates(data[quoted_instrument.ticker], quoted_instrument, time_range, split_time)
        for quoted_instrument in config.research.quoted_instruments:
            self.__check_quoted_instrument_dates(data[quoted_instrument.ticker], quoted_instrument, time_range, split_time)
            

        return wf.CommandState.SUCCESS


    def __check_quoted_instrument_dates(self, data: dlm.ComplexData,
                                        quoted_instrument: cfgm.QuotedInstrument,
                                        time_range: cfgm.TimeRange,
                                        split_time: datetime):
        self.LOGGER.info(f"Checking dates for instrument '{quoted_instrument.ticker}'...")
        data_to_check = data.remote_data.loaded_data if self.__use_remote_data else data.local_data.loaded_data
        instrument_begin_time = data_to_check.index.min()
        instrument_end_time = data_to_check.index.max()

        if instrument_begin_time > time_range.begin_time:
            raise ValueError(f"Instrument '{quoted_instrument.ticker}' begin time must be not later than the range [{time_range.begin_time}, {time_range.end_time}], but is {instrument_begin_time}")
        if instrument_end_time < time_range.end_time:
            raise ValueError(f"Instrument '{quoted_instrument.ticker}' end time must be not earlier than the range [{time_range.begin_time}, {time_range.end_time}], but is {instrument_end_time}")


class SelectDataByTimeRangeCommand(wf.AbstractCommand):

    LOGGER = clog.get_logger('SelectDataByTimeRangeCommand')

    def __init__(self,
                 use_remote_data: bool=True,
                 selected_data_context_name: str='selected-data'):
        super().__init__()
        self.__use_remote_data = use_remote_data
        self.__selected_data_context_name = selected_data_context_name

    def execute(self, context: dict):
        config: cfgm.Config = context['config']
        time_range = config.research.machine_learning.time_range

        self.LOGGER.info(f"Executing SelectDataByTimeRangeCommand. Target time range: {time_range}")

        data: OrderedDict[str, dlm.ComplexData] = context['data']
        selected_data = OrderedDict[str, pd.DataFrame]()
        quoted_instrument = config.research.target_quoted_instrument
        selected_dataframe = self.__select_instrument_databy_dates_range(
            data[quoted_instrument.ticker], quoted_instrument, time_range
        )
        selected_data[quoted_instrument.ticker] = selected_dataframe
        for quoted_instrument in config.research.quoted_instruments:
            selected_dataframe = self.__select_instrument_databy_dates_range(
                data[quoted_instrument.ticker], quoted_instrument, time_range
            )
            selected_data[quoted_instrument.ticker] = selected_dataframe

        context[self.__selected_data_context_name] = selected_data
        
        return wf.CommandState.SUCCESS

    def __select_instrument_databy_dates_range(self, data: dlm.ComplexData,
                                        quoted_instrument: cfgm.QuotedInstrument,
                                        time_range: cfgm.TimeRange) -> pd.DataFrame:
        dataframe = data.remote_data.loaded_data if self.__use_remote_data else data.local_data.loaded_data

        self.LOGGER.info(f"['{quoted_instrument.ticker}'] Selecting data... original time range: \
            {dataframe.index.min()} - {dataframe.index.max()}")

        selected_dataframe = dataframe[
            (dataframe.index >= time_range.begin_time) & (dataframe.index <= time_range.end_time)
        ].copy(deep=True)

        self.LOGGER.info(f"['{quoted_instrument.ticker}'] Selected range: {selected_dataframe.index.min()} - {selected_dataframe.index.max()}")
        return selected_dataframe


class DataClearingCommand(wf.AbstractCommand):

    LOGGER = clog.get_logger('DataClearingCommand')

    def __init__(self, selected_data_context_name: str='selected-data'):
        super().__init__()
        self.__selected_data_context_name = selected_data_context_name

    def execute(self, context: dict):
        self.LOGGER.info(f"Clearing/interpolating selected data...")
        config: cfgm.Config = context['config']

        time_range = config.research.machine_learning.time_range
        selected_data: OrderedDict[str, pd.DataFrame] = context[self.__selected_data_context_name]

        instrument = config.research.target_quoted_instrument
        interpolated_data = self.__clear_and_interpolate_data(selected_data[instrument.ticker], instrument, time_range)
        if not interpolated_data.empty:
            selected_data[instrument.ticker] = interpolated_data
        for instrument in config.research.quoted_instruments:
            interpolated_data = self.__clear_and_interpolate_data(selected_data[instrument.ticker], instrument, time_range)
            if not interpolated_data.empty:
                selected_data[instrument.ticker] = interpolated_data

        return wf.CommandState.SUCCESS

    def __clear_and_interpolate_data(self, dataframe: pd.DataFrame,
                                     instrument: cfgm.QuotedInstrument,
                                     time_range: cfgm.TimeRange) -> pd.DataFrame:
        if not instrument.data_transformation.clearing:
            self.LOGGER.info(f"['{instrument.ticker}'] No data clearing configuration.")
            return pd.DataFrame()

        if 'missing_values' in instrument.data_transformation.clearing:
            missing_values_strategy = instrument.data_transformation.clearing['missing_values']
            if missing_values_strategy == 'interpolate_by_previous_date':
                self.LOGGER.info(
                    f"['{instrument.ticker}'] Clearing and interpolating data...\n\
            Begin time: {dataframe.index.min()}, End time: {dataframe.index.max()};\n\
            Shape: {dataframe.shape};\n\
            NaN rows:\n{dataframe.isna().sum()}"
                    )

                new_index_range = pd.date_range(time_range.begin_time, time_range.end_time, freq='D', inclusive='both')
                new_index_range_list = new_index_range.to_list()
                dataframe = dataframe.reindex(new_index_range, fill_value=np.nan)
                self.LOGGER.info(
                    f"['{instrument.ticker}'] Reindexed data, timerange={time_range}...\n\
            Index First date: {new_index_range_list[0]}; Index Last date: {new_index_range_list[-1]};\n\
            Begin time: {dataframe.index.min()}, End time: {dataframe.index.max()};\n\
            Shape: {dataframe.shape};\n\
            NaN rows:\n{dataframe.isna().sum()}"
                    )

                dataframe.interpolate(method='pad', inplace=True)
                self.LOGGER.info(f"['{instrument.ticker}'] Interpolated data.\n\
            Begin time: {dataframe.index.min()}, End time: {dataframe.index.max()}'\n\
            Shape: {dataframe.shape};\n\
            NaN rows:\n{dataframe.isna().sum()}")

                return dataframe
            else:
                raise ValueError(f"['{instrument.ticker}'] Invalid missing values strategy '{missing_values_strategy}'")


class PreparedDataReportCommand(wf.AbstractCommand):
    
    LOGGER = clog.get_logger('PreparedDataReportCommand')
    
    def __init__(self, selected_data_context_name: str='selected-data'):
        super().__init__()
        self.__selected_data_context_name = selected_data_context_name

    def execute(self, context: dict):
        config: cfgm.Config = context['config']
        selected_data: OrderedDict[str, pd.DataFrame] = context[self.__selected_data_context_name]

        time_range = config.research.machine_learning.time_range        
        self.LOGGER.info(f"['{time_range}'] Preparing report...")
        
        template = templates.TemplateFactory.make_template('01-prepared_data_report_text.jinja2')
        report_text = templates.render_with(
            template,
            {
                'research': config.research,
                'machine_learning': config.research.machine_learning,
                'target_quoted_instrument': config.research.target_quoted_instrument,
                'quoted_instruments': config.research.quoted_instruments,
                'data': selected_data
            }
        )

        self.LOGGER.info(report_text)

        return wf.CommandState.SUCCESS


class DataTreatingCommand(wf.AbstractCommand):
    
    LOGGER = clog.get_logger('DataTreatingCommand')

    def __init__(self, selected_data_context_name: str='selected-data'):
        super().__init__()
        self.__selected_data_context_name = selected_data_context_name

    def execute(self, context: dict):
        config: cfgm.Config = context['config']
        research = config.research
        selected_data: OrderedDict[str, pd.DataFrame] = context[self.__selected_data_context_name]

        instrument = research.target_quoted_instrument
        instrument_data = selected_data[instrument.ticker]
        self.__treat_data(instrument, instrument_data)
        for instrument in research.quoted_instruments:
            instrument_data = selected_data[instrument.ticker]
            self.__treat_data(instrument, instrument_data)

        return wf.CommandState.SUCCESS

    def __treat_data(self, instrument: cfgm.QuotedInstrument, instrument_data: pd.DataFrame):
        self.LOGGER.info(f"[{instrument.ticker}] Trying to treat data...")
        if instrument.data_transformation.treatment:
            if 'dimensionality_reduction' in instrument.data_transformation.treatment:
                self.LOGGER.info(f"[{instrument.ticker}] Dimensionality reduction selected...")
                self.__reduce_dimentionality(instrument, instrument_data)
            else:
                self.LOGGER.warn(f"[{instrument.ticker}] Treatment config is awkward: {instrument.data_transformation.treatment}")
        else:
            self.LOGGER.info(f"[{instrument.ticker}] Treatment config is empty: {instrument.data_transformation.treatment}")

    def __reduce_dimentionality(self, instrument: cfgm.QuotedInstrument, instrument_data: pd.DataFrame):
        dimentionality_reduction = instrument.data_transformation.treatment['dimensionality_reduction']
        self.LOGGER.info(f"[{instrument.ticker}] Dimensionality reduction selected... {dimentionality_reduction}...")
        if dimentionality_reduction in [None, 'None', 'none']:
            # Handling null
            pass
        elif dimentionality_reduction == 'OHLC':
            self.__reduce_dimentionality_OHLC(instrument, instrument_data)
        elif dimentionality_reduction == 'HLC':
            self.__reduce_dimentionality_HLC(instrument, instrument_data)
        else:
            raise ValueError(f"['{instrument.ticker}'] Invalid dimentionality reduction strategy '{dimentionality_reduction}'")


    def __reduce_dimentionality_OHLC(self, instrument: cfgm.QuotedInstrument, instrument_data: pd.DataFrame):
        data = instrument_data
        column_names = data.columns.to_list()
        self.LOGGER.info(f"[{instrument.ticker}][dimentionality_reduction][OHLC] All columns: {column_names}")
        column_open = colit.find_first(column_names, key=lambda x: 'open' in x)
        column_high = colit.find_first(column_names, key=lambda x: 'high' in x)
        column_low  = colit.find_first(column_names, key=lambda x: 'low' in x)
        column_close= colit.find_first(column_names, key=lambda x: 'close' in x)
        self.LOGGER.info(f"[{instrument.ticker}][dimentionality_reduction][OHLC] columns: \
            {', '.join([column_open, column_high, column_low, column_close])}")

        data['ohlc'] = 0.25 * (
            data[column_open] + data[column_high] + data[column_low] + data[column_close]
        )
        data.drop(columns=[column_open, column_high, column_low, column_close], inplace=True)
        self.LOGGER.info(f"[{instrument.ticker}][dimentionality_reduction][OHLC] Complete. Columns: {instrument_data.columns}")

    def __reduce_dimentionality_HLC(self, instrument: cfgm.QuotedInstrument, instrument_data: pd.DataFrame):
        data = instrument_data
        column_names = data.columns.to_list()
        column_open = colit.find_first(column_names, key=lambda x: 'open' in x)
        column_high = colit.find_first(column_names, key=lambda x: 'high' in x)
        column_low  = colit.find_first(column_names, key=lambda x: 'low' in x)
        column_close= colit.find_first(column_names, key=lambda x: 'close' in x)
        self.LOGGER.info(f"[{instrument.ticker}][dimentionality_reduction][HLC] columns: \
            {', '.join([column_open, column_high, column_low, column_close])}")

        data['hlc'] = 0.25 * (
            data[column_high] + data[column_low] + data[column_close]
        )
        data.drop(columns=[column_open, column_high, column_low, column_close], inplace=True)
        self.LOGGER.info(f"[{instrument.ticker}][dimentionality_reduction][HLC] Complete. Columns: {instrument_data.columns}")


class JoinedDatasetCommand(wf.AbstractCommand):

    LOGGER = clog.get_logger('DatasetCommand')

    def __init__(self, selected_data_context_name: str, dataset_context_name: str):
        super().__init__()
        self.__selected_data_context_name = selected_data_context_name
        self.__dataset_context_name = dataset_context_name

    def execute(self, context: dict):
        config: cfgm.Config = context['config']
        research = config.research
        selected_data: OrderedDict[str, pd.DataFrame] = context[self.__selected_data_context_name]

        self.LOGGER.info(f"[DATASET] Joining columns from selected data: {self.__selected_data_context_name}")

        data_dictionary = selected_data
        joined_data: pd.DataFrame = data_dictionary[research.target_quoted_instrument.ticker].copy(deep=True)
        for quoted_instrument in research.quoted_instruments:
            right_data = data_dictionary[quoted_instrument.ticker]
            right_suffix = '_' + quoted_instrument.ticker
            right_data = right_data.add_suffix(right_suffix)
            self.LOGGER.info(f"[DATASET] [{quoted_instrument.ticker}] Right joining... right_suffix = '{right_suffix}'")
            joined_data = joined_data.join(
                right_data,
                how='outer'
            )
            
        self.LOGGER.info(f"[DATASET] Joined columns column_types:\n{joined_data.info()}")

        context[self.__dataset_context_name] = joined_data

        return wf.CommandState.SUCCESS