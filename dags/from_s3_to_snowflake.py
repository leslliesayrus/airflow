import os
from airflow.decorators import dag
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
from astro import sql as aql
from astro.files import File
from astro.constants import FileType
from astro.sql.table import Table, Metadata

args = {
    'owner': 'Lesllie Sayrus',
    'retries': 1,
    'retries_delay': 0
}

LANDING_ZONE = 's3://bucket-silver-layer/teste/'
AWS_DEFAULT = 'aws_default'
SNOWFLAKE = 'snowflake_default'



@dag(
dag_id= 's3_to_snowflake',
start_date= datetime(2022,12,4),
schedule_interval= timedelta(hours=1),
max_active_runs= 1,
catchup= False,
default_args= args,
tags = ['aws', 's3', 'snowflake'],
)

def main_function():

    init = DummyOperator(task_id = 'start')

    
    load_data = aql.load_file(
        input_file = File(path = LANDING_ZONE + 'heroes', 
                                filetype = FileType.CSV, conn_id = AWS_DEFAULT),
        output_table = Table(name = 'heroes', conn_id = SNOWFLAKE),
        task_id = 'load_heroes',
        if_exists = 'replace',
        use_native_support = True,
        columns_names_capitalization = 'original'

    )

    end = DummyOperator(task_id = 'end')


    init >> load_data >> end

dag = main_function()


