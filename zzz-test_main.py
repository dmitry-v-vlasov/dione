from collections import OrderedDict

import config_logging as clog
import config_model as cfgm
import config as cfg
import data_load_model as dlm
import data_load as dl
import workflow as wf
import workflow_stage as wfs


config_loader = cfg.ConfigLoader.from_path('./config/config.yaml')
config = config_loader.load_config()

workflow_context = {'config': config}
data_load_command = wfs.DataLoadCommand()
data_tending_command = wfs.DataTendingCommand()

commands = OrderedDict[str, wf.AbstractCommand]()
commands['data_loading'] = data_load_command
commands['data_tending'] = data_tending_command

workflow = wf.Workflow(commands=commands)
workflow.execute(workflow_context)

