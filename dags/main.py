from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import requests
import zipfile
import io
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType
from pyspark.sql.functions import col
from clickhouse_driver import Client

# Функция для загрузки CSV файла
def read_file():
    base_url = 'https://cloud-api.yandex.net/v1/disk/public/resources/download?'
    public_key = 'https://disk.yandex.ru/d/bhf2M8C557AFVw'  # Ваша публичная ссылка

    # Получаем информацию о ресурсе
    resource_info_url = f'https://cloud-api.yandex.net/v1/disk/public/resources?public_key={public_key}'
    resource_info_response = requests.get(resource_info_url)

    if resource_info_response.status_code == 200:
        resource_info = resource_info_response.json()
        print("Содержимое по публичной ссылке:")
        print(resource_info)  # Выводим информацию о ресурсе

        # Получаем ссылку на файл
        items = resource_info['_embedded']['items']
        zip_file_url = None
        for item in items:
            if item['name'] == 'archive (12).zip':
                zip_file_url = item['file']
                break

        if zip_file_url:
            # Загружаем ZIP-архив
            download_response = requests.get(zip_file_url)

            if download_response.status_code == 200:
                # Проверяем, существует ли папка tmp, и создаем ее, если нет
                tmp_dir = '/tmp'
                if not os.path.exists(tmp_dir):
                    os.makedirs(tmp_dir)

                # Открываем ZIP-архив из загруженного содержимого
                with zipfile.ZipFile(io.BytesIO(download_response.content)) as z:
                    # Извлекаем все CSV файлы из архива
                    for file_info in z.infolist():
                        if file_info.filename.endswith('.csv'):
                            with z.open(file_info.filename) as csv_file:
                                with open(os.path.join(tmp_dir, file_info.filename), 'wb') as output_file:
                                    output_file.write(csv_file.read())
                            print(f"Файл {file_info.filename} успешно сохранен в папку {tmp_dir}.")
            else:
                print(f'Ошибка при загрузке файла: {download_response.status_code}, {download_response.text}')
        else:
            print("Файл 'archive (12).zip' не найден в ресурсе.")
    else:
        print(f'Ошибка при получении информации о ресурсе: {resource_info_response.status_code}, {resource_info_response.text}')


# Объединенная функция для загрузки, валидации и преобразования данных
def process_data_and_validate():
    file_path = "/tmp/russian_houses.csv"
    file_utf8_path = "/tmp/russian_houses_utf8.csv"
    parquet_output_path = "/tmp/russian_houses.parquet"  # Определяем переменную здесь    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Файл не найден: {file_path}")

    # Перекодировка файла из UTF-16LE в UTF-8
    with open(file_path, 'r', encoding='utf-16le') as f:
        content = f.read()

    with open(file_utf8_path, 'w', encoding='utf-8') as f:
        f.write(content)

    spark = SparkSession.builder.appName("DataLoader").getOrCreate()

    # Загрузка данных
    df = spark.read.csv(file_utf8_path, header=True, inferSchema=True)
    print(f"Количество строк: {df.count()}")
    print("Имена столбцов:", df.columns)

    # Удаляем строки с пустыми значениями в maintenance_year
    cleaned_df = df.filter(df["maintenance_year"].isNotNull() & (df["maintenance_year"] != "") & (df["maintenance_year"].cast(IntegerType()).isNotNull()))

    # Выводим количество пустых строк
    empty_rows_count = df.count() - cleaned_df.count()
    print(f"Количество пустых строк: {empty_rows_count}")

    # Проверяем, есть ли пустые строки
    if empty_rows_count > 0:
        print("Пустые строки:")
        df.filter(F.col("maintenance_year").isNull() | F.col("square").isNull() | F.col("region").isNull()).show()

    # Валидация данных
    assert cleaned_df.count() > 0, "DataFrame пустой"

    # Преобразование типов данных
    transformed_df = cleaned_df.select(
        cleaned_df["maintenance_year"].cast(IntegerType()).alias("maintenance_year"),
        cleaned_df["region"].cast("string").alias("region"),
        cleaned_df["square"].cast(FloatType()).alias("square")
    )

    # 4. Вычислите средний и медианный год постройки зданий
    avg_year = transformed_df.agg(F.avg("maintenance_year")).first()[0]
    median_year = transformed_df.approxQuantile("maintenance_year", [0.5], 0.01)[0]
    print(f"Средний год постройки: {avg_year}, Медианный год постройки: {median_year}")

    # 5. Определите топ-10 областей и городов с наибольшим количеством объектов
    top_regions = transformed_df.groupBy("region").count().orderBy(F.desc("count")).limit(10)
    print("Топ-10 областей и городов с наибольшим количеством объектов:")
    top_regions.show()

    # 6. Найдите здания с максимальной и минимальной площадью в рамках каждой области
    min_max_square = transformed_df.groupBy("region").agg(
        F.min("square").alias("Минимальная площадь"),
        F.max("square").alias("Максимальная площадь")
    )
    print("Минимальная и максимальная площадь зданий в каждой области:")
    min_max_square.show()

    # 7. Определите количество зданий по десятилетиям
    decade_counts = transformed_df.withColumn("Десятилетие", (F.floor(transformed_df["maintenance_year"] / 10) * 10).cast(IntegerType())) \
                                   .groupBy("Десятилетие").count() \
                                   .orderBy("Десятилетие")
    print("Количество зданий по десятилетиям:")
    decade_counts.show()

    # Сохранение DataFrame в формате Parquet
    transformed_df.write.mode("overwrite").parquet(parquet_output_path)
    print(f"DataFrame сохранен в {parquet_output_path}")

    return parquet_output_path  # Возвращаем путь к сохраненному файлу


# Функция для загрузки данных в ClickHouse
def load_to_clickhouse(parquet_file_path):
    client = Client(host='clickhouse_user', port=9000)  # Укажите адрес вашего ClickHouse
    # Читаем данные из Parquet
    spark = SparkSession.builder.appName("DataLoader").getOrCreate()
    df = spark.read.parquet(parquet_file_path)
    
    # Выводим имена столбцов для отладки
    print("Имена столбцов:", df.columns)

    # Преобразуем значения в maintenance_year в целые числа, заменяя некорректные на None
    df = df.withColumn("maintenance_year", col("maintenance_year").cast("integer"))

    # Преобразуем DataFrame в список словарей для вставки в ClickHouse
    records = [{**row.asDict()} for row in df.collect()]

    # Вставка данных в ClickHouse
    client.execute('INSERT INTO buildings VALUES', records)
    print("Данные успешно загружены в ClickHouse.")


# Функция для извлечения топ-25 домов с площадью больше 60 кв.м
def get_top_25_houses():
    client = Client(host='clickhouse_user', port=9000)  # Укажите адрес вашего ClickHouse
    query = """
    SELECT *
    FROM buildings
    WHERE square > 60
    ORDER BY square DESC
    LIMIT 25
    """
    top_houses = client.execute(query)
    print("Топ-25 домов с площадью больше 60 кв.м:")
    for house in top_houses:
        print(house)


# DAG
with DAG(
    dag_id='main',
    start_date=datetime(2023, 10, 1),
    schedule_interval='@daily',
    catchup=False,
    default_args={
        'execution_timeout': timedelta(minutes=30),  # Установите общий таймаут для всех задач
    }
) as dag:
    read_file_task = PythonOperator(
        task_id='read_file_task',
        python_callable=read_file,
        execution_timeout=timedelta(minutes=30)  # Таймаут для этой задачи
    )

    process_data_task = PythonOperator(
        task_id='process_data_task',
        python_callable=process_data_and_validate,
        do_xcom_push=True,  # Позволяет передавать данные в следующую задачу
        execution_timeout=timedelta(minutes=30)  # Таймаут для этой задачи
    )

    load_to_clickhouse_task = PythonOperator(
        task_id='load_to_clickhouse_task',
        python_callable=load_to_clickhouse,
        op_kwargs={'parquet_file_path': process_data_task.output},  # Передаем путь к Parquet файлу
        execution_timeout=timedelta(minutes=10)  # Таймаут для этой задачи
    )

    get_top_25_houses_task = PythonOperator(
        task_id='get_top_25_houses_task',
        python_callable=get_top_25_houses,
        execution_timeout=timedelta(minutes=10)  # Таймаут для этой задачи
    )

    read_file_task >> process_data_task >> load_to_clickhouse_task >> get_top_25_houses_task
