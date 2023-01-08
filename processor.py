from collections import OrderedDict

from typing import Callable

import config_logging as clog
import workflow as wf
import config_model as cfgm
import config as cfg


class Processor(object):

    LOGGER = clog.get_logger('Processor')

    def __init__(self, config_loader: cfg.ConfigLoader, workflow: wf.Workflow, worflow_context: OrderedDict):
        self.__config_loader = config_loader
        self.__config = self.__config_loader.load_config()
        self.__workflow = workflow
        self.__workflow_context = worflow_context.copy()
        self.__init_workflow_context()

    def __init_workflow_context(self):
        self.__workflow_context['config'] = self.__config

    def process(self):
        self.LOGGER.info('Start of processing')
        self.workflow.execute(self.__workflow_context)
        self.LOGGER.info('Finished processing')

    def process_next_stage(self):
        self.LOGGER.info('Start of processing next stage...')
        try:
            self.__workflow.execute_next_stage(self.__workflow_context)
        except Exception as exception:
            self.LOGGER.error(f"Error on the stage {self.__workflow.current_stage}: {exception}", exc_info=True)
        finally:
            self.LOGGER.info('Finished processing next stage')

    def process_previous_stage(self):
        self.LOGGER.info('Start of processing previous stage...')
        try:
            self.__workflow.execute_previous_stage(self.__workflow_context)
        except Exception as exception:
            self.LOGGER.error(f"Error on the stage {self.__workflow.current_stage}: {exception}", exc_info=True)
        finally:
            self.LOGGER.info('Finished processing previous stage')

    @property
    def config(self) -> cfgm.Config:
        return self.__config

    @property
    def current_workflow_stage(self) -> str:
        return self.__workflow.current_stage
    
    @property
    def current_workflow_state(self) -> wf.CommandState:
        return self.__workflow.current_state
    
    