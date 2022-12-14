from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 tables = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)  
        
        for tab in self.tables:
            records = redshift.get_records("SELECT COUNT(*) FROM {}".format(tab))  
            
            if len(records) < 1 or len(records[0]) < 1:
                self.log.error("{} returned no results".format(tab))
                raise ValueError("Data quality check failed. {} returned no results".format(tab))
                
            num = records[0][0]
            if num == 0:
                self.log.error("No records present in destination table {}".format(tab))
                raise ValueError("No records present in destination {}".format(tab))
               
            self.log.info("Data quality on table {} check passed with {} records".format(table, num_records))