from airflow import DAG
from airflow.utils.dates import days_ago
from dataverk_airflow import notebook_operator, python_operator
from airflow.models import Variable
import logging

dmp_host = Variable.get('MARKEDSPLASSEN_HOST_DEV', default_var=None)

if dmp_host:
    os.environ["MARKEDSPLASSEN_HOST_DEV"] = dmp_host


with DAG('test_dag', 
         start_date=days_ago(1), 
         schedule="0 8 * * 1-5", 
         catchup=False) as dag:
    py_op = python_operator(
         dag=dag,
         name="refresh_datagrunnlaget",
         repo="navikt/poao-ventetid",
         branch="master",
         allowlist=["dm08-scan.adeo.no:1521"],
         script_path="python/test_dag.py",
         requirements_path="requirements.txt",
         retries=0,
         slack_channel="#team-effekt-tech",
         use_uv_pip_install=True,
    )

