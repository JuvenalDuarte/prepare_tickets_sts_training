from pycarol import Carol, Staging
from ..functions.ingestion import ingest_tickets
from ..functions.delete_data import delete_data
from ..flow.commons import Task
import luigi
import logging

logger = logging.getLogger(__name__)
luigi.auto_namespace(scope=__name__)

# Saves data to Carol 
def send_data_to_carol(df, staging_name, connector_name, crosswalk):
    staging = Staging(Carol())
    staging.send_data(
        staging_name=staging_name, 
        connector_name=connector_name, 
        data=df, 
        async_send=True,
        storage_only=True, 
        force=True,
        crosswalk_auto_create=crosswalk,
        gzip=True,
        auto_create_schema=True,
        flexible_schema=False,
    )

    return

class IngestTickets(Task):
    out_connector = luigi.Parameter()
    datetime = luigi.Parameter() 
    undersampling = luigi.BoolParameter() 
    preproc = luigi.Parameter()

    def easy_run(self, inputs):
        subject2title, subject2question = ingest_tickets()

        if len(subject2title) > 0:
            conn = self.out_connector
            stag = "ticketsubject_articletitle_pairs"

            # This output table is writen only as a reference for available articles on the API.
            # We run a full load everytime we need to update it.
            logger.info(f'Cleaning staging {conn}/{stag}.')
            delete_data(staging_name=stag, connector_name=conn, full_load=True)

            logger.info(f'Saving {len(subject2title)} records to {conn}/{stag}.')
            send_data_to_carol(subject2title, 
                            staging_name=stag, 
                            connector_name=conn,
                            crosswalk=["ticket_subject", "article_title"])

            stag = "ticketsubject_articlequestion_pairs"

            # This output table is writen only as a reference for available articles on the API.
            # We run a full load everytime we need to update it.
            logger.info(f'Cleaning staging {conn}/{stag}.')
            delete_data(staging_name=stag, connector_name=conn, full_load=True)

            logger.info(f'Saving {len(subject2question)} records to {conn}/{stag}.')
            send_data_to_carol(subject2question, 
                            staging_name=stag, 
                            connector_name=conn,
                            crosswalk=["ticket_subject", "article_question"])

            logger.info(f'Finished.')

        return subject2title, subject2question