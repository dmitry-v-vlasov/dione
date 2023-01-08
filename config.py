from os import path
import pprint

import yaml

import config_logging
import config_model as model


class ConfigLoader:
    """Load configuration"""

    LOGGER = config_logging.get_logger('ConfigLoader')

    def __init__(self, config_file_path: str) -> None:
        self.config_file_path = config_file_path

    def load_config(self) -> model.Config:
        with open(self.config_file_path) as config_file:
            config_dictionary = yaml.load(
                config_file, Loader=yaml.FullLoader
            )
            self.LOGGER.info(f"Loaded configuration:\n{pprint.pformat(config_dictionary)}")

            model_config = model.Config.parse_obj(
                config_dictionary['config'])
            self.LOGGER.info(f"Loaded configuration models:\n{model_config}")

            return model_config

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

