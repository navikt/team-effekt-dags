import os
from airflow import DAG
from airflow.utils.dates import days_ago
from dataverk_airflow import notebook_operator, python_operator, quarto_operator
from airflow.models import Variable
import logging
from kubernetes import client

with DAG('ventetidsindikatoren', 
         start_date=days_ago(1), 
         schedule="0 7 6-8 * *", 
         catchup=False) as dag:
         py_op = python_operator(
                  dag=dag,
                  name="refresh_datagrunnlaget",
                  repo="navikt/poao-ventetid",
                  branch="master",
                  allowlist=["dmv03-scan.adeo.no:1521"],
                  script_path="python/refresh_datagrunnlaget.py",
                  requirements_path="requirements.txt",
                  retries=0,
                  slack_channel= Variable.get('SLACK_ALERT_CHANNEL'),
                  use_uv_pip_install=True,
         )
                  
         quarto_op1 = quarto_operator(
                  dag=dag,
                  name="quarto_datafortelling",
                  repo="navikt/poao-ventetid",
                  python_version="3.10",
                  quarto={
                  "folder": "notebooks/ventetid_dvh_raw_data/quarto",
                  "env": "prod",
                  "id": "1c513c0b-bf77-4599-97d1-74be17a4a9b3",
                  "token": Variable.get('NADA_TOKEN'),},
                  branch="master",
                  requirements_path="requirements.txt",
                  slack_channel= Variable.get('SLACK_ALERT_CHANNEL'),
                  use_uv_pip_install=True,
                  allowlist=["dmv03-scan.adeo.no:1521"],
                  retries=0,
                  resources=client.V1ResourceRequirements(
                  requests={"memory": "10G", "cpu": "1500m"},
                  limits={"memory": "15G", "cpu": "2000m"},
                  )
         )

# dependencies

py_op >> quarto_op1 >> quarto_op2
