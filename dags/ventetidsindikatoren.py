from airflow import DAG
from airflow.utils.dates import days_ago
from dataverk_airflow import notebook_operator, python_operator


with DAG('ventetidsindikatoren', start_date=days_ago(1), schedule="0 8 * * 1-5", catchup=False) as dag:
    nb_op = python_operator(
        dag=dag,
        name="ventetidsindikatoren",
        repo="navikt/poao-ventetid",
        script_path="python/test.py",
        #nb_path="notebooks/ventetid_dvh_raw_data/fetch_raw_dvh_sky.ipynb",
        #requirements_path="requirements.txt",
        #slack_channel="{{ var.value.get('SLACK_ALERT_CHANNEL') }}",
    )
