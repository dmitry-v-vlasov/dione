from os import path
import pprint
import textwrap
from io import StringIO

import yaml

import config_logging
import config_model as model


class ConfigLoader:
    """Load configuration"""

    LOGGER = config_logging.get_logger('ConfigLoader')

    def __init__(self, config_file_path: str) -> None:
        self.config_file_path = config_file_path

    def load_config(self, print_short_report=True, print_verbose_report: bool=False) -> model.Config:
        with open(self.config_file_path) as config_file:
            config_dictionary = yaml.load(
                config_file, Loader=yaml.FullLoader
            )
            if print_verbose_report:
                self.LOGGER.info(f"Loaded configuration:\n{pprint.pformat(config_dictionary)}")

            config = model.Config.parse_obj(
                config_dictionary['config'])
            if print_verbose_report:
                self.LOGGER.info(f"Loaded configuration models:\n{config}")
            if print_short_report:
                target_instrument = config.research.target_quoted_instrument
                quoted_instruments = config.research.quoted_instruments

                sio = StringIO()
                sio.write(
f"""
/================================================\\
Loaded configuration short report:
------------------------------------------------
Research.
name: {config.research.name}
1. Target instrument.
    ticker: {target_instrument.ticker}
    name:   {target_instrument.name}
    description:
        {textwrap.fill(target_instrument.description, width=80, initial_indent=' '*4, subsequent_indent=' '*8)}""")

                sio.write(
f"""
2. Extra quoted instruments.""")
                for quoted_instrument in quoted_instruments:
                    sio.write(
f"""
    - Quoted instrument
        ticker: {quoted_instrument.ticker}
        name:   {quoted_instrument.name}
        description:
            {textwrap.fill(quoted_instrument.description, width=80, initial_indent=' '*4, subsequent_indent=' '*12)}""")

                machine_learning = config.research.machine_learning
                sio.write(
f"""
3. Machine learning:
    ⤇ begin time: {machine_learning.time_range.begin_time}
    ⥥ split date: {machine_learning.split_time}
    ⤆ end time: {machine_learning.time_range.end_time}
    ▒▒░ cross-validation strategy: {machine_learning.cross_validation_strategy}""")
                sio.write(
"""
\\================================================/""")
                self.LOGGER.info(sio.getvalue())

            return config

    @classmethod
    def from_path(cls, config_file_path: str):
        """Create a new ConfigLoader"""
        if not path.isfile(config_file_path):
            raise FileNotFoundError(
                f'Config file {config_file_path} not found.')

        cls.LOGGER.info(f'Found config file: {config_file_path}')

        config_loader = cls.__new__(cls)
        config_loader.config_file_path = config_file_path
        return config_loader

