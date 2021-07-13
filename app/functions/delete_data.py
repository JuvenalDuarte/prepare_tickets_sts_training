from pycarol import Carol, CDSStaging, Tasks
import logging
import time
import random
import pandas as pd

logger = logging.getLogger(__name__)

def delete_data(staging_name, connector_name, full_load):

    if not full_load:
        return pd.DataFrame(columns=['empty'])

    carol = Carol()

    if connector_name is None:
        raise ValueError('connector_name must be set for entity==staging')

    try:
        cds = CDSStaging(carol)
        carol_task = Tasks(carol)
        task_name = f'Deleting Data from Staging Table: {staging_name}'
        task_id = cds.delete(staging_name=staging_name, connector_name=connector_name)
        task_id = task_id['taskId']

        while carol_task.get_task(task_id).task_status in ['READY', 'RUNNING']:
            # avoid calling task api all the time.
            time.sleep(round(12 + random.random() * 5, 2))
            logger.debug(f'Running {task_name}')

        task_status = carol_task.get_task(task_id).task_status

        if task_status == 'COMPLETED':
            logger.info(f'Task {task_id} for {connector_name}/{task_name} completed.')

        elif task_status in ['FAILED', 'CANCELED']:
            logger.error(f'Something went wrong while processing: {connector_name}/{task_name}')
            raise ValueError(f'Something went wrong while processing: {connector_name}/{task_name}')
    except:
        logger.error(f'Unable to drop records on {staging_name}. Skipping delete data.')

    return pd.DataFrame(columns=['empty'])