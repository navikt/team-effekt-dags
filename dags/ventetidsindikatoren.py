from airflow import DAG
from airflow.utils.dates import days_ago
from dataverk_airflow import notebook_operator, python_operator
from airflow.models import Variable
import logging


with DAG('ventetidsindikatoren', 
         start_date=days_ago(1), 
         schedule="0 8 * * 1-5", 
         catchup=False) as dag:
    py_op = python_operator(
         dag=dag,
         name="refresh_datagrunnlaget",
         repo="navikt/poao-ventetid",
         branch="master",
         allowlist=["dm08-scan.adeo.no:1521"],
         script_path="python/refresh_datagrunnlaget.py",
         requirements_path="requirements.txt",
         retries=0,
         #slack_channel="#team-effekt-tech",
    )
    nb_op = notebook_operator(
         dag=dag,
         name="run_notebook",
         repo="navikt/poao-ventetid",
         branch="master",
         allowlist=["dm08-scan.adeo.no:1521"],
         nb_path="notebooks/ventetid_dvh_raw_data/fetch_raw_dvh_sky.ipynb",
         requirements_path="requirements.txt",
         retries=0,
         #slack_channel=Variable.get("SLACK_ALERT_CHANNEL"),
         use_uv_pip_install=True,
    )

    # dependencies
                  
    py_op >> nb_op
