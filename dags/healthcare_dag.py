from airflow.decorators import dag
from pendulum import datetime
from airflow.providers.standard.operators.bash import BashOperator
from airflow.models import Variable

from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
PATH_TO_DATA_SCRIPT = "/usr/local/airflow/include/raw_data_generation/healthcare_data.py"
PATH_TO_SQL_SCRIPT="/usr/local/airflow/include/raw_data_generation/create_external_tables.sql"
with open(PATH_TO_SQL_SCRIPT,'r') as f:
    CREATE_EXTERNAL_TABLES_SQL=f.read()

GCP_CONN_ID = Variable.get("GCP_CONN_ID", default_var="gcp")

@dag(dag_id='airflow-dbt-gcp-pipeline',
schedule='@daily',
start_date=datetime(2021,1,1),
catchup=False,tags=['healthcare'],
description='Healthcare pipeline',
doc_md=__doc__)


def dbt_healthcare_pipeline():
    generate_data = BashOperator(
        task_id="generate_data",
        bash_command=f"python {PATH_TO_DATA_SCRIPT}"
    )

    create_external_tables = BigQueryInsertJobOperator(
        task_id="create_external_tables",
        configuration={
            "query": {
                "query": CREATE_EXTERNAL_TABLES_SQL,
                "useLegacySql": False,
            }
        },
        location="EU",  
        gcp_conn_id=GCP_CONN_ID
    )

    dbt_test_raw = BashOperator(
        task_id="dbt_test_raw",
        bash_command="dbt test --select source:*",
        cwd="/usr/local/airflow/include/healthcare_dbt_gcp"
    )
    transform = BashOperator(
        task_id="transform",
        bash_command="dbt run --select path:models",
        cwd="/usr/local/airflow/include/healthcare_dbt_gcp"
    )
  
    generate_data >> create_external_tables >> dbt_test_raw >> transform
   
    

dbt_healthcare_pipeline()

    



