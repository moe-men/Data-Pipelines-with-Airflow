from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 tab = "",
                 sql = "",  
                 appendOnly = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.table = tab
        self.sql = sql
        self.append_only = appendOnly

    def execute(self, context):
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if not self.append_only:
            self.log.info("Delete {} dimension table".format(self.table))
            redshift.run("DELETE FROM {}".format(self.table))    
            
        self.log.info("Insert data from staging table into {} dimension table".format(self.table))
        
        insert_statement = f"INSERT INTO {self.table} \n{self.sql}"
        self.log.info(f"Running sql: \n{insert_statement}")
        redshift.run(insert_statement)
        self.log.info(f"Successfully completed insert into {self.table}")
