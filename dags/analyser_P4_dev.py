
import os
from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from dataverk_airflow import quarto_operator
from kubernetes import client
#import logging




with DAG('analyser_P4_dev', start_date=days_ago(1), schedule="15 8 * * 1-5", catchup=False) as dag:
    quarto_op = quarto_operator(
        dag=dag,
        name="Analyser_Ventetid_P4",
        repo="navikt/poao-ventetid",
        python_version="3.10",
        quarto={
            "folder": "notebooks/ventetid_dvh_raw_data/analyse_manuscript",
            "env": "dev",
            "id": "871b1027-4dc1-4fd7-9c90-c34281b34c69",
            "token": Variable.get("NADA_TOKEN_DEV"),
        },
        branch="master",
        requirements_path="requirements.txt",
        slack_channel= Variable.get('SLACK_ALERT_CHANNEL'),
        use_uv_pip_install=True,
        allowlist=["dmv03-scan.adeo.no:1521", "datamarkedsplassen.intern.dev.nav.no:443"],
        #retries=0,
        resources=client.V1ResourceRequirements(
            requests={"memory": "20G", "cpu": "3000m"},
            limits={"memory": "40G", "cpu": "5000m"},
        ),
        startup_timeout_seconds=600,
        
    )


quarto_op

