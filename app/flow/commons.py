import luigi

luigi.interface.InterfaceLogging.setup(luigi.interface.core())

import os
import logging
import traceback

logger = logging.getLogger(__name__)

from pycarol.pipeline import Task
from luigi import Parameter
from datetime import datetime
from pycarol import Carol
from pycarol.apps import Apps

PROJECT_PATH = os.getcwd()
TARGET_PATH = os.path.join(PROJECT_PATH, 'luigi_targets')
Task.TARGET_DIR = TARGET_PATH
#Change here to save targets locally.
Task.is_cloud_target = True
Task.version = Parameter()
Task.resources = {'cpu': 1}

now = datetime.now()
now_str = now.isoformat()
_settings = Apps(Carol()).get_settings()

# Defines the connector, in the current environment, where
# training sets will be writen to.
out_connector = _settings.get('output_connector')

# Applies random undersampling to the majority class
undersampling = _settings.get('undersampling_mode')

# Only tickets with the provided satisfaction rates will be used
satisfaction_filter = _settings.get('satisfaction_filter')

# Alows basic preproc = only regularize encodings; or advanced preproc = remove special chars, set lowercase and remove stopwords.
preproc = _settings.get('preproc_mode')


@Task.event_handler(luigi.Event.FAILURE)
def mourn_failure(task, exception):
    """Will be called directly after a failed execution
       of `run` on any JobTask subclass
    """
    logger.error(f'Error msg: {exception} ---- Error: Task {task},')
    traceback_str = ''.join(traceback.format_tb(exception.__traceback__))
    logger.error(traceback_str)


@Task.event_handler(luigi.Event.PROCESSING_TIME)
def print_execution_time(self, processing_time):
    logger.debug(f'### PROCESSING TIME {processing_time}s. Output saved at {self.output().path}')


#######################################################################################################

params = dict(
    version=os.environ.get('CAROLAPPVERSION', 'dev'),
    datetime = now_str,
    out_connector=out_connector,
    undersampling = undersampling,
    preproc = preproc,
    satisfaction_filter = satisfaction_filter
)
