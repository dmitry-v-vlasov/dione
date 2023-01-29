from IPython.core.display import display
from IPython.core.display import HTML

from collections import OrderedDict

import config_logging as clog
import config_model as cfgm
import config as cfg
import data_load_model as dlm
import data_load as dl
import workflow as wf
import workflow_stage as wfs


config_loader = cfg.ConfigLoader.from_path('./config/config.yaml')
config = config_loader.load_config(print_short_report=True, print_verbose_report=False)

workflow_context = {'config': config}
data_load_command = wfs.DataLoadCommand()
data_tending_command = wfs.DataTendingCommand()
eda_post_tending_command = wfs.AutoEdaCommand(
    context_data_name='data',
    use_remote_data=True,
    eda_name='01-eda_post_tending',
    report_joined=True
)
# No missing values (NaNs) here
# No EDA after missing value treatment.
check_dates_command = wfs.CheckDatesCommand(
    use_remote_data=True
)
select_data_by_time_range_command = wfs.SelectDataByTimeRangeCommand(
    use_remote_data=True,
    selected_data_context_name='selected-data'
)
clear_data_command = wfs.DataClearingCommand(
    selected_data_context_name='selected-data'
)
prepared_data_report_command = wfs.PreparedDataReportCommand(
    selected_data_context_name='selected-data'
)
treat_data_command = wfs.DataTreatingCommand(
    selected_data_context_name='selected-data'
)
eda_post_treating_command = wfs.AutoEdaCommand(
    context_data_name='selected-data',
    use_remote_data=False,
    eda_name='09-eda_post_treating',
    report_joined=True
)

dataset_command = wfs.JoinedDatasetCommand(
    selected_data_context_name='selected-data',
    dataset_context_name='dataset',
    save_datasets=True
)


commands = OrderedDict[str, wf.AbstractCommand]()
commands['01-data_loading'] = data_load_command
commands['02-data_tending'] = data_tending_command
# commands['03-eda_post_tending_command'] = eda_post_tending_command
commands['04-check_dates_command'] = check_dates_command
commands['05-select_data_by_timerange_command'] = select_data_by_time_range_command
commands['06-clear_data_command'] = clear_data_command
commands['07-prepared_data_report_command'] = prepared_data_report_command
commands['08-treat_data_command'] = treat_data_command
# commands['09-eda_post_treating_command'] = eda_post_treating_command
commands['11-dataset_command'] = dataset_command


workflow = wf.Workflow(commands=commands)
workflow.execute(workflow_context)
