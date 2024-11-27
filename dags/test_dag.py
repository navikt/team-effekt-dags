import os
from airflow import DAG
from airflow.utils.dates import days_ago
from dataverk_airflow import notebook_operator, python_operator, quarto_operator
from airflow.models import Variable
import logging


dmp_host = Variable.get('MARKEDSPLASSEN_HOST_DEV', default_var=None)
if dmp_host:
    os.environ["MARKEDSPLASSEN_HOST"] = dmp_host


with DAG('test_dag', 
         start_date=days_ago(1), 
         schedule="0 8 * * 1-5", 
         catchup=False) as dag:
    #py_op = python_operator(
    #     dag=dag,
    #     name="test_dag",
    #     repo="navikt/poao-ventetid",
    #     branch="master",
    #     allowlist=["dm08-scan.adeo.no:1521"],
    #     script_path= Variable.get('SCRIPT_PATH_TEST'),
    #     #requirements_path="requirements.txt",
    #     retries=0,
    #     slack_channel="#team-effekt-tech",
    #     use_uv_pip_install=True,
    #),
    quarto_op = quarto_operator(
        dag=dag,
        name="quarto-op",
        repo="navikt/poao-ventetid",
        branch="master",
        allowlist=["dm08-scan.adeo.no:1521"],
        retries=0,
        quarto={
            "path": "notebooks/ventetid_dwh_raw_data/ventetid_MVP.ipynb",
            "env": "dev",
            "id": "9f3e0ca2-fb56-4122-a8c5-453e45fb18f4",
            "token": Variable.get('NADA_TOKEN_DEV'),
        },
        requirements_path="requirements.txt",
        slack_channel= Variable.get('SLACK_ALERT_CHANNEL'),
    )



