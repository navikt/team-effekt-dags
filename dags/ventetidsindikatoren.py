from airflow import DAG
from airflow.utils.dates import days_ago
from dataverk_airflow import notebook_operator


with DAG('DataverkAirflowNotebook', start_date=days_ago(1), schedule="0 8 * * 1-5", catchup=False) as dag:
    nb_op = notebook_operator(
        dag=dag,
        name="nb-op",
        repo="navikt/poao-ventetid",
        nb_path="notebooks/ventetid_dvh_raw_data/fetch_raw_dvh_sky.ipynb",
        requirements_path="requirements.txt",
        #slack_channel="{{ var.value.get('SLACK_ALERT_CHANNEL') }}",
    )
