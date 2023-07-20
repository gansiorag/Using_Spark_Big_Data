import airflow
import copy
import datetime
from airflow.models import DAG, Variable
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook
from typing import Any
import datetime
import psycopg2


KERBEROS_BASE_PYTHON_CRED = {
    'host': '10.127.130.22',
    'port': 5432,
    'database': 'gudata',
    'user': 'bigdata',
    'password': '5FyEpgEZGFT2CKzs'
}

def check_connection_dbase():
   with psycopg2.connect(**KERBEROS_BASE_PYTHON_CRED) as base_conn:
 
  
       cur = base_conn.cursor()
 
       cur.execute('SELECT * FROM gudata.fns_asur_2ndfl_vw LIMIT 2;')
       allDuplicates = cur.fetchall()
       base_conn.close()



DEFAULT_ARGS = {
    "dag_id": "kd_check_conn_bases",
    "start_date": airflow.utils.dates.days_ago(1),
    "catchup": False,
    "schedule_interval": "00 04 * * 4"
}

kwargs = {}
kwargs['task_name'] = 'check_connection_dbase'
# kwargs['connections'] = {'destination': {'airflow_conn_id': 'dgen_vertica_prod_rnd', 'schema': 'kd_import', 'table': '', 'db_type': 'vertica', 'addition_params': None}, 
#                          'increment': {}, 
#                          'dv_connections': {}}

with DAG(**DEFAULT_ARGS) as dag:
    s_address_raw_conditional_number: Any = PythonOperator(
    task_id="check_connection_dbase",
    run_as_user="sys_kd_hub",
    op_kwargs=copy.deepcopy(kwargs),
    python_callable=check_connection_dbase
    )


[s_address_raw_conditional_number]
