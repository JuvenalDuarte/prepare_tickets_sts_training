# Imports
from dotenv import load_dotenv
load_dotenv('.env', override=True)

import sys
from time import time

from app.flow import commons, ingestion

def get_tasks():

    task_list = [
        ingestion.IngestTickets(**commons.params)
    ]
    

    return task_list


if __name__ == '__main__':

    task_list = get_tasks()

    t0 = time()
    for task in task_list:
        commons.logger.debug(f'Starting task: "{task}')
        exec_status = commons.luigi.build([task], local_scheduler=True, workers=1,
                                          scheduler_port=8880,
                                          detailed_summary=True)

        commons.logger.debug(f'Finished {task}, Elapsed time: {time() - t0}')
        if exec_status.status.name == 'SUCCESS_WITH_RETRY' or exec_status.status.name == 'SUCCESS':
            continue
        else:
            commons.logger.error(f'Error: Elapsed time: {time() - t0}')
            sys.exit(1)

    commons.logger.info(f'Elapsed time: {time() - t0}')