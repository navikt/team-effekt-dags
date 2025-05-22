
import os
from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from dataverk_airflow import quarto_operator
from kubernetes import client
#import logging




with DAG('analyser_P4', start_date=days_ago(1), schedule="15 8 * * 1-5", catchup=False) as dag:
    quarto_op = quarto_operator(
        dag=dag,
        name="Analyser_Ventetid_P4",
        repo="navikt/poao-ventetid",
        python_version="3.10",
        quarto={
            "folder": "notebooks/ventetid_dvh_raw_data/analyse_manuscript",
            "env": "prod",
            "id": "0e3b5c4b-4560-484e-a9b6-153d09c0e55e",
            "token": Variable.get("NADA_TOKEN"),
        },
        branch="master",
        requirements_path="requirements.txt",
        slack_channel= Variable.get('SLACK_ALERT_CHANNEL'),
        use_uv_pip_install=True,
        allowlist=["dmv09-scan.adeo.no:1521", "datamarkedsplassen.intern.nav.no:443"],
        #retries=0,
        resources=client.V1ResourceRequirements(
            requests={"memory": "20G", "cpu": "3000m"},
            limits={"memory": "40G", "cpu": "5000m"},
        ),
        startup_timeout_seconds=600,
        
    )


quarto_op
