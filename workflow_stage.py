import typing
from collections import OrderedDict

import strings as ustr
import config_logging as clog
import config_model as cfgm
import data_load as dl
import data_load_model as dlm
import workflow as wf


class DataLoadCommand(wf.AbstractCommand):

    LOGGER = clog.get_logger('DataLoadCommand')

    def __init__(self):
        super().__init__()

    def execute(self, context: dict):
        config: cfgm.Config = context['config']
        data = OrderedDict[str, dlm.ComplexData]()

        target_quoted_instrument = config.research.target_quoted_instrument
        target_quoted_instrument_data_loader = \
            dl.StrategyBasedDataLoader(target_quoted_instrument)
        target_quoted_instrument_data = target_quoted_instrument_data_loader.load_data()
        data[target_quoted_instrument.ticker] = target_quoted_instrument_data
        self.LOGGER.info(f"Loaded data for the target quoted instrument '{target_quoted_instrument.ticker}.' \
            DataFrame shape: {target_quoted_instrument_data.remote_data.loaded_data.shape}")

        for quoted_instrument in config.research.quoted_instruments:
            quoted_instrument_data_loader = \
                dl.StrategyBasedDataLoader(quoted_instrument)
            quoted_instrument_data = quoted_instrument_data_loader.load_data()
            data[quoted_instrument.ticker] = quoted_instrument_data
            self.LOGGER.info(f"Loaded data for the quoted instrument '{quoted_instrument.ticker}.' \
                DataFrame shape: {quoted_instrument_data.remote_data.loaded_data.shape}")

        context['data'] = data
        return wf.CommandState.SUCCESS



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
        if 'columns' in tending_config:
            if 'remove' in tending_config['columns']:
                columns = tending_config['columns']['remove']
                if columns:
                    remote_dataframe.drop(columns=columns, inplace=True)
                    local_dataframe.drop(columns=columns, inplace=True)
            if 'names' in tending_config['columns']:
                if tending_config['columns']['names']['to_snake_case']:
                    remote_dataframe.rename(
                        columns={column_name: ustr.to_snake_case(column_name) for column_name in remote_dataframe.columns.to_list()},
                        inplace=True
                    )
                    local_dataframe.rename(
                        columns={column_name: ustr.to_snake_case(column_name) for column_name in local_dataframe.columns.to_list()},
                        inplace=True
                    )
        self.LOGGER.info(f"Tended quoted instrument '{quoted_instrument.ticker}': \
            Column names: remote - {remote_dataframe.columns}, local - {local_dataframe.columns}")
        

class AutoEdaCommand(wf.AbstractCommand):
    
    LOGGER = clog.get_logger('AutoEdaCommand')
    
    def __init__(self):
        super().__init__()

    def execute(self, context: dict):
        pass