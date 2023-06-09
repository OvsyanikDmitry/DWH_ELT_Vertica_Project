import boto3
import botocore
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from vertica_python import connect
from airflow.operators.python_operator import PythonOperator



default_args = {
    'owner': 'mityaov',
    'start_date': datetime(2022, 10, 1),
    'depends_on_past': False,
    'end_date': datetime(2022, 10, 31),  
}

#Postgres conn
pg_conn_id = 'postgres_default'
pg_hook = PostgresHook(postgres_conn_id=pg_conn_id)
conn_p = pg_hook.get_conn()
cur_p = conn_p.cursor() 
#Vertica conn 
vertica_conn_id = 'vertica_default'
vertica_conn_params = Variable.get('vertica_conn_params', deserialize_json=True)
vertica_conn = connect(
    host=vertica_conn_params['host'],
    port=vertica_conn_params['port'],
    database=vertica_conn_params['database'],
    user=vertica_conn_params['user'],
    password=vertica_conn_params['password'],
    
)
cur_v = vertica_conn.cursor()
#S3 conn
s3_conn_params = Variable.get('aws', deserialize_json=True)
s3 = boto3.client(
    's3',
    aws_access_key_id=s3_conn_params["aws_access_key_id"],
    aws_secret_access_key=s3_conn_params["aws_secret_access_key"],
    endpoint_url='https://storage.yandexcloud.net/'
)

#Источник Postgres
def upload_stg_transactions(cur_p, date):
    #Проверяем наличие данных для вставки
    count_query = open('/lessons/sql/count_transactions_query.sql').read().replace('{date}', date)
    cur_p.execute(count_query)
    count_result = cur_p.fetchone()
    #Если данных нет, то скрипт начинает обработку следующей даты
    if count_result[0] == 0:
        print(f'Нет данных для даты: {date}.')
        return

    #Экспортируем данные из БД в CSV во временное хранилище контейнера
    select_query = open('/lessons/sql/stg_select_transactions_query.sql').read().replace('{date}', date)
    with open(f'transactions-{date}.csv', 'w') as f:
        cur_p.copy_expert(
            f''' COPY ({select_query}) TO STDOUT DELIMITER ',' ''', f)

    print(f'Получены данные из PostgreSQL - {date}')
    #Загружаем полученные данные 
    load_transactions_queries = open(
        '/lessons/sql/stg_load_transactions_to_vertica.sql').read().replace('{date}', date).split(';')
    for load_transactions_query in load_transactions_queries:
        cur_v.execute(load_transactions_query)

    vertica_conn.close()


#Источник S3 
def extract_and_load_currencies(date):
    #Форматирование даты в нужный формат
    date_str = datetime.strptime(date, '%Y-%m-%d').strftime('%Y-%m-%d')

    try:
        #Скачивание файла currencies_history.csv из S3
        file_path = '/tmp/currencies_history.csv'
        s3.download_file('final-project', 'currencies_history.csv', file_path)
        #Чтение данных из файла CSV
        with open(file_path, 'r') as csv_file:
            lines = csv_file.readlines()[1:]  #Пропустить первую строку(заголовки)
        #Создание списка кортежей для вставки в Vertica
        data = []
        for line in lines:
            row = line.strip().split(',')
            #Приведение типа данных для столбца "currency_code" к int
            row[0] = int(row[0])
            data.append(row)

        with vertica_conn.cursor() as cur:
            #Удалить старые записи из таблицы
            cur.execute("DELETE FROM OVSYANIKDMITRYYANDEXRU__STAGING.CURRENCIES WHERE date_update::date = '{date}';".format(date=date_str))
            #Проверка на существование временной таблицы и её удаление, если она уже существует
            cur.execute("DROP TABLE IF EXISTS temp_currencies")
            #Создание временной таблицы для загрузки данных
            cur.execute("CREATE TEMPORARY TABLE temp_currencies (currency_code INT, currency_code_with INT, date_update DATE, currency_with_div FLOAT)")
            #Вставка данных во временную таблицу
            for row in data:
                cur.execute("INSERT INTO temp_currencies VALUES (%s, %s, %s, %s)", row)
            #Вставка данных из временной таблицы в основную таблицу
            cur.execute("INSERT INTO OVSYANIKDMITRYYANDEXRU__STAGING.CURRENCIES SELECT * FROM temp_currencies")
            #Удаление временной таблицы
            cur.execute("DROP TABLE temp_currencies")
        vertica_conn.commit()

    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print(f"File not found: currencies-{date_str}.csv")
        else:
            raise


#Заполняем Витрину 
def datamart_global_metrics(date):
    date_str = datetime.strptime(date, '%Y-%m-%d').strftime('%Y-%m-%d')
    with vertica_conn as conn:
        with conn.cursor() as cur:
            cdm_global_metrics_queries = open(f'/lessons/sql/datamart_global_metrics.sql').read().replace('{date}', date_str)
            queries = cdm_global_metrics_queries.split(';')
            for query in queries:
                if query.strip():  
                    logging.info(f"Executing query: {query}")
                    cur.execute(query)
                    logging.info("Query executed successfully")
                    conn.commit()



with DAG(
    "Currencies-transactions-datamart",
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2022, 10, 1),
    end_date = datetime(2022, 10, 31), 
    catchup=True
) as dag:    
    
    extract_and_load_transactions_task = PythonOperator(
        task_id="extract_and_load_transactions_task",
        python_callable=upload_stg_transactions,
        op_kwargs={'cur_p': cur_p,
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
