import config_logging as clog
import config_model as cfgm
import config as cfg
import data_load_model as dlm
import data_load as dl
import workflow_stage as wfs


config_loader = cfg.ConfigLoader.from_path('./config/config.yaml')
config = config_loader.load_config()

workflow_context = {'config': config}
data_load_command = wfs.DataLoadCommand()
data_load_command.execute(workflow_context)
