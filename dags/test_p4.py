#Tester om quarto-book-operatoren fungerer som den skal. 

import os
from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from dataverk_airflow import quarto_operator
from kubernetes import client
#import logging




with DAG('test_dag_p4', start_date=days_ago(1), schedule="15 8 * * 1-5", catchup=False) as dag:
    quarto_op = quarto_operator(
        dag=dag,
        name="Tester_ny versjon av tidsindikatoren",
        repo="navikt/poao-ventetid",
        python_version="3.10",
        quarto={
            "folder": "notebooks/ventetid_dvh_raw_data/quarto",
            "env": "dev",
            "id": "dcd6746c-6f14-47fc-b496-dc6c4ef54ba7",
            "token": Variable.get("NADA_TOKEN_DEV"),
        },
        branch="master",
        requirements_path="requirements.txt",
        #slack_channel= Variable.get('SLACK_ALERT_CHANNEL'),
        use_uv_pip_install=True,
        allowlist=["dmv09-scan.adeo.no:1521", "datamarkedsplassen.intern.dev.nav.no:443"],
        #retries=0,
        resources=client.V1ResourceRequirements(
            requests={"memory": "10G", "cpu": "1500m"},
            limits={"memory": "15G", "cpu": "2000m"},
        ),
        startup_timeout_seconds=600,
        
    )


quarto_op
