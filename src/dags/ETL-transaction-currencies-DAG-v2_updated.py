import boto3
import botocore
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from vertica_python import connect
from airflow.operators.python_operator import PythonOperator

#Postgres conn
def create_postgres_connection():
    pg_conn_id = 'postgres_default'
    pg_hook = PostgresHook(postgres_conn_id=pg_conn_id)
    conn_p = pg_hook.get_conn()
    cur_p = conn_p.cursor()
    return cur_p

#Vertica conn 
def create_vertica_connection():
    vertica_conn_params = Variable.get('vertica_conn_params', deserialize_json=True)
    vertica_conn = connect(
        host=vertica_conn_params['host'],
        port=vertica_conn_params['port'],
        database=vertica_conn_params['database'],
        user=vertica_conn_params['user'],
        password=vertica_conn_params['password'],
    )
    cur_v = vertica_conn.cursor()
    return cur_v

#S3 conn
def create_s3_client():
    s3_conn_params = Variable.get('aws', deserialize_json=True)
    s3 = boto3.client(
        's3',
        aws_access_key_id=s3_conn_params["aws_access_key_id"],
        aws_secret_access_key=s3_conn_params["aws_secret_access_key"],
        endpoint_url='https://storage.yandexcloud.net/'
    )
    return s3

#Источник Postgres
def upload_stg_transactions(cur_p, date):
    count_query = open('/lessons/sql/count_transactions_query.sql').read().replace('{date}', date)
    cur_p.execute(count_query)
    count_result = cur_p.fetchone()
    if count_result[0] == 0:
        logging.info(f'Нет данных для даты {date}.')
        return

    select_query = open('/lessons/sql/stg_select_transactions_query.sql').read().replace('{date}', date)
    with open(f'transactions-{date}.csv', 'w') as f:
        cur_p.copy_expert(
            f''' COPY ({select_query}) TO STDOUT DELIMITER ',' ''', f)
    logging.info(f'Данные получены из PostgreSQL-{date}')

    try:
        cur_v = create_vertica_connection()
        load_transactions_queries = open(
            '/lessons/sql/stg_load_transactions_to_vertica.sql').read().replace('{date}', date).split(';')
        for load_transactions_query in load_transactions_queries:
            cur_v.execute(load_transactions_query)
        cur_v.close()
        logging.info(f'Данные о транзакциях загружены в Vertica {date}')
    except Exception as e:
        logging.error(f'Ошибка при загрузке данных транзакций в Vertica:{str(e)}')

#Источник S3 
def extract_and_load_currencies(date):
    date_str = datetime.strptime(date, '%Y-%m-%d').strftime('%Y-%m-%d')
    try:
        file_path = '/tmp/currencies_history.csv'
        s3 = create_s3_client()
        s3.download_file('final-project', 'currencies_history.csv', file_path)

        with open(file_path, 'r') as csv_file:
            lines = csv_file.readlines()[1:]

        data = []
        for line in lines:
            row = line.strip().split(',')
            row[0] = int(row[0])
            data.append(row)

        cur_v = create_vertica_connection()
        with cur_v:
            cur_v.execute("DELETE FROM OVSYANIKDMITRYYANDEXRU__STAGING.CURRENCIES WHERE date_update::date = %s;", (date_str,))
            cur_v.execute("DROP TABLE IF EXISTS temp_currencies")
            cur_v.execute( "CREATE TEMPORARY TABLE temp_currencies (currency_code INT, currency_code_with INT, date_update DATE, currency_with_div FLOAT)")

        for row in data:
            cur_v.execute("INSERT INTO temp_currencies VALUES (%s, %s, %s, %s)", row)

        cur_v.execute("INSERT INTO OVSYANIKDMITRYYANDEXRU__STAGING.CURRENCIES SELECT * FROM temp_currencies")
        cur_v.execute("DROP TABLE temp_currencies")

        logging.info(f'Данные currencies извлечены и загружены в Vertica {date}')
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            logging.error(f"Файл не найден: currencies-{date_str}.csv")
        else:
            logging.error(f'Ошибка при извлечении и загрузке данных currencies: {str(e)}')
    except Exception as e:
        logging.error(f'Ошибка при извлечении и загрузке данных currencies: {str(e)}')

#Заполняем Витрину 
def datamart_global_metrics(date):
    date_str = datetime.strptime(date, '%Y-%m-%d').strftime('%Y-%m-%d')
    try:
        cur_v = create_vertica_connection()
        with cur_v:
            cdm_global_metrics_queries = open(f'/lessons/sql/datamart_global_metrics.sql').read().replace('{date}', date_str)
            queries = cdm_global_metrics_queries.split(';')
            for query in queries:
                if query.strip():
                    logging.info(f"Выполнение запроса {query}")
                    cur_v.execute(query)
                    logging.info("Успешно")
            logging.info(f'datamart_global-metrics загружена {date}')
    except Exception as e:
        logging.error(f'Ошибка расчета и загрузки datamart_global-metrics: {str(e)}')


def create_dag():
    default_args = {
        'owner': 'mityaov',
        'start_date': datetime(2022, 10, 1),
        'depends_on_past': False,
        'end_date': datetime(2022, 10, 31),
    }
    with DAG(
        "Currencies-transactions-datamart",
        default_args=default_args,
        schedule_interval='@daily',
        start_date=datetime(2022, 10, 1),
        end_date=datetime(2022, 10, 31),
        catchup=True
    ) as dag:

            extract_and_load_transactions_task = PythonOperator(
            task_id="extract_and_load_transactions_task",
            python_callable=upload_stg_transactions,
            op_kwargs={'cur_p': create_postgres_connection(),
                       'date': '{{ ds }}'}
        )

            extract_and_load_currencies_task = PythonOperator(
            task_id="extract_and_load_currencies_task",
            python_callable=extract_and_load_currencies,
            op_kwargs={'date': '{{ ds }}'},
            provide_context=True
        )

            mart_global_metrics_task = PythonOperator(
            task_id="datamart_global_metrics",
            python_callable=datamart_global_metrics,
            op_kwargs={'date': '{{ ds }}'},
            dag=dag
        )
            extract_and_load_transactions_task>>extract_and_load_currencies_task>>mart_global_metrics_task



