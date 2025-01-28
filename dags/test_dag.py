#Tester om quarto-book-operatoren fungerer som den skal. 

import os
from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from dataverk_airflow import quarto_operator
#from kubernetes import client
#import logging


dmp_host = Variable.get('MARKEDSPLASSEN_HOST_DEV', default_var=None)
if dmp_host:
    os.environ["MARKEDSPLASSEN_HOST"] = dmp_host


with DAG('DataverkAirflowQuartoBook', start_date=days_ago(1), schedule="15 8 * * 1-5", catchup=False) as dag:
    quarto_op = quarto_operator(
        dag=dag,
        name="Tester_quartobook_website",
        repo="navikt/poao-ventetid",
        python_version="3.10",
        quarto={
            "folder": "notebooks/ventetid_dvh_raw_data/quarto",
            "env": "dev",
            "id": "f454d4ac-50b2-47b9-a8c8-bd786b9d663f",
            "token": Variable.get("NADA_TOKEN_DEV"),
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



# with DAG('test_dag', 
#          start_date=days_ago(1), 
#          schedule="0 8 * * 1-5", 
#          catchup=False) as dag:
             
#     quarto_op = quarto_operator(
#         dag=dag,
#         name="MVP_datafortelling",
#         repo="navikt/poao-ventetid",
#         python_version="3.10",
#         quarto={
#             "path": "notebooks/ventetid_dvh_raw_data/ventetid_MVP.ipynb",
#             "env": "prod",
#             "id": "1c513c0b-bf77-4599-97d1-74be17a4a9b3",
#             "token": Variable.get('NADA_TOKEN'),
#         },
#         branch="master",
#         requirements_path="requirements.txt",
#         slack_channel= Variable.get('SLACK_ALERT_CHANNEL'),
#         use_uv_pip_install=True,
#         allowlist=["dm08-scan.adeo.no:1521"],
#         retries=0,
#         resources=client.V1ResourceRequirements(
#             requests={"memory": "10G", "cpu": "1500m"},
#             limits={"memory": "15G", "cpu": "2000m"},
#         ),
#         startup_timeout_seconds=600,
#     )

quarto_op
