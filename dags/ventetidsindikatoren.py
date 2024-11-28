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
         slack_channel= Variable.get('SLACK_ALERT_CHANNEL'),
         use_uv_pip_install=True,
    ),
    quarto_op = quarto_operator(
        dag=dag,
        name="MVP_datafortelling",
        repo="navikt/poao-ventetid",
        python_version="3.10",
        quarto={
            "path": "notebooks/ventetid_dvh_raw_data/ventetid_MVP.ipynb",
            "env": "prod",
            "id": "1c513c0b-bf77-4599-97d1-74be17a4a9b3",
            "token": Variable.get('NADA_TOKEN'),
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

    # dependencies
                  
    py_op >> quarto_op
