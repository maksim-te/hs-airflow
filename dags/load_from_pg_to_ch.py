from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable

from hooks.clickhouse_hook import ClickHouseHook

from datetime import datetime, timedelta
from psycopg2.extras import RealDictCursor


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2021, 1, 5),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG("load_from_pg_to_ch",
          default_args=default_args,
          schedule_interval=timedelta(minutes=5),
          catchup=False,
          max_active_runs=1
          )


def load_data():
    target_schema = 'default'
    target_tab = 'hs_events'

    ch_hook = ClickHouseHook('dwh-ch')
    last_loaded_id = ch_hook.select_data(f'select max(id) as max_id from {target_schema}.{target_tab}')[0]['max_id']
    pg_hook = PostgresHook('hs_pg')
    pg_conn = pg_hook.get_conn()

    pg_query = f"""
    select  
    dal.id,
    dal.action_time,
    au.id as user_id,
    au.date_joined as join_date,
    case when au.first_name is null then 'Not defined' else au.first_name end as name,
    case when au.email is null then 'Not defined' else au.email end as email, 
    cast (dal.object_id as integer)as task_id,
    dal.action_flag
    from public.django_admin_log dal
    inner join public.django_content_type dct on dal.content_type_id = dct.id 
    inner join public.auth_user au  on dal.user_id = au.id 
    where dct.model = 'task'
    and dal.id > {last_loaded_id}
    """

    print(pg_query)

    with pg_conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(pg_query)
        data = cursor.fetchall()
        ch_hook.insert_data(database=target_schema, table=target_tab, values=data)


load_data_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)