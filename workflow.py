import typing
from enum import Enum

from collections import OrderedDict

import config_logging


class CommandState(Enum):
    INITIALIZED = -1
    FAILED = 0
    SUCCESS = 1
    ABORTED = 2
    FINISHED = 3


class AbstractCommand(object):
    """Parent class of all concrete commands."""

    def execute(self, context: dict) -> CommandState:
        """throws a NotImplementedError"""
        raise NotImplementedError('Not implemented!')


class DummyCommand(AbstractCommand):
    """Concrete Command # 1: Child class of AbstractCommand"""
 
    def execute(self, request) -> CommandState:
        return CommandState.ABORTED


class Workflow(AbstractCommand):

    LOGGER = config_logging.get_logger('Workflow')

    def __init__(self, commands: OrderedDict[str, AbstractCommand], context: dict):
        self.__commands = commands
        self.__workflow_stages = list(self.__commands.keys())
        self.__workflow_position = -1
        self.__workflow_state = CommandState.INITIALIZED

    def execute(self, context: dict) -> CommandState:
        self.LOGGER.info('Executing workflow entirely...')
        for workflow_stage, workflow_command in self.__commands.items():
            workflow_state = self.execute_next_stage(context)
            if workflow_state == CommandState.FAILED or workflow_state == CommandState.ABORTED:
                self.LOGGER.warn(f"Workflow execution is interrupted. State: {workflow_state}")
                return workflow_state
        self.LOGGER.info('Executing workflow entirely... Done.')
        return self.__workflow_state

    def execute_next_stage(self, context: dict):
        self.__execute_sibling_stage(context, direction=1)

    def execute_previous_stage(self, context: dict):
        self.__execute_sibling_stage(context, direction=-1)

    def __execute_sibling_stage(self, context: dict, direction: int) -> CommandState:
        assert direction != 0
        assumed_position_shift = 1 if direction > 0 else -1
        assumed_workflow_position = self.__workflow_position + assumed_position_shift

        if assumed_workflow_position <= 0 or assumed_workflow_position >= len(self.__workflow_stages):
            return self.__workflow_state

        workflow_stage = self.__workflow_stages[assumed_workflow_position]
        workflow_command = self.__commands[workflow_stage]

        self.LOGGER.info(f"\t - executing workflow stage {workflow_stage} ...")
        self.__workflow_state = workflow_command.execute(context)
        self.LOGGER.info(f"\t   ... Done. State: {self.__workflow_state}.")

        return self.__workflow_state

    @property
    def current_stage(self) -> str:
        return self.__workflow_stages[self.__workflow_position]

    @property
    def current_state(self) -> CommandState:
        return self.__workflow_state
