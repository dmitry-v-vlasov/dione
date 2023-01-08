import workflow as wf
import config_model as cfgm
import data_load as dl


class DataLoadCommand(wf.AbstractCommand):
    def __init__(self):
        super().__init__()

    def execute(self, context: dict):
        config: cfgm.Config = context['config']

        target_quoted_instrument_data_loader = \
            dl.StrategyBasedDataLoader(config.research.target_quoted_instrument)
        target_quoted_instrument_data_loader.load_data()

        for quoted_instrument in config.research.quoted_instruments:
            quoted_instrument_data_loader = \
                dl.StrategyBasedDataLoader(quoted_instrument)
            quoted_instrument_data_loader.load_data()