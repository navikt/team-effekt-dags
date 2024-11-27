import os
from airflow import DAG
from airflow.utils.dates import days_ago
from dataverk_airflow import notebook_operator, python_operator, quarto_operator
from airflow.models import Variable
import logging
from kubernetes import client


dmp_host = Variable.get('MARKEDSPLASSEN_HOST_DEV', default_var=None)
if dmp_host:
    os.environ["MARKEDSPLASSEN_HOST"] = dmp_host


with DAG('test_dag', 
         start_date=days_ago(1), 
         schedule="0 8 * * 1-5", 
         catchup=False) as dag:
             
    quarto_op = quarto_operator(
        dag=dag,
        name="MVP_datafortelling",
        repo="navikt/poao-ventetid",
        quarto={
            "path": "notebooks/ventetid_dvh_raw_data/ventetid_MVP.ipynb",
            "env": "dev",
            "id": "9f3e0ca2-fb56-4122-a8c5-453e45fb18f4",
            "token": Variable.get('NADA_TOKEN_DEV'),
        },
        branch="master",
        requirements_path="requirements.txt",
        slack_channel= Variable.get('SLACK_ALERT_CHANNEL'),
        use_uv_pip_install=True,
        allowlist=["dm08-scan.adeo.no:1521"],
        retries=0,
        resources=client.V1ResourceRequirements(
            requests={"memory": "10G", "cpu": "1500m"},
            limits={"memory": "15G", "cpu": "2000m"},
        ),
        startup_timeout_seconds=600,
    )

quarto_op
