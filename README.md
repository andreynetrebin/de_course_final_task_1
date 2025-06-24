# Проект: Анализ данных с использованием PySpark и ClickHouse
![de_course_task1](https://github.com/user-attachments/assets/88e99936-111f-4877-a9ba-e3aa499620e3)
## Содержание

- [Описание](#описание)
- [Задание](#задание)
- [Блок-схема](#блок-схема)
- [Стек технологий](#cтек-технологий)
- [Установка](#установка)
- [Использование](#использование)

## Описание
Этот проект предназначен для анализа данных с использованием PySpark и ClickHouse. Он включает в себя загрузку данных, их обработку и анализ, а также загрузку результатов в базу данных ClickHouse. Проект организован с использованием Apache Airflow для управления задачами.

## Задание

1. Обратите внимание, что в проекте используются PySpark и ClickHouse. PySpark в Airflow в Docker Compose отсутствует, его необходимо добавить самостоятельно.
2. Предварительно разверните Docker Compose из первых шагов. Шаги 1-7 должны быть выполнены в DAG.
3. Загрузите файл данных в DataFrame PySpark. Обязательно выведите количество строк.
4. Убедитесь, что данные корректно прочитаны (правильный формат, отсутствие пустых строк).
5. Преобразуйте текстовые и числовые поля в соответствующие типы данных (например, дата, число).
6. Вычислите средний и медианный год постройки зданий.
7. Определите топ-10 областей и городов с наибольшим количеством объектов.
8. Найдите здания с максимальной и минимальной площадью в рамках каждой области.
9. Определите количество зданий по десятилетиям (например, сколько зданий построено в 1950-х, 1960-х и т.д.).
10. Создайте схему таблицы в ClickHouse, которая будет соответствовать структуре ваших данных. Это можно сделать не через Airflow.
11. Настройте соединение с ClickHouse из скрипта, учтите, что сделать это необходимо в Airflow.
12. Загрузите обработанные данные из DataFrame в таблицу в ClickHouse (Airflow).
13. Выполните SQL скрипт в Python, который выведет топ 25 домов, у которых площадь больше 60 кв.м (Airflow).

## Блок-схема
![de_course_task1_блок_схема](https://github.com/user-attachments/assets/901ce075-a80b-4b9a-8d73-2da18e465adf)

## Стек технологий

- Apache Airflow
- PySpark
- ClickHouse
- Docker

## Установка

1. **Клонируйте репозиторий:**

   ```bash
   git clone https://github.com/andreynetrebin/de_course_final_task_1.git
   cd de_course_final_task_1
   
2. **Создайте и активируйте виртуальное окружение (опционально):**

   ```bash
    python -m venv venv
    source venv/bin/activate  # Для Linux/Mac
    venv\Scripts\activate     # Для Windows

3. **Создайте и активируйте виртуальное окружение (опционально):**

   ```bash
    apache-airflow
    pyspark
    psycopg2-binary
    requests
   
4. **Настройте Docker Compose:**

   ```yaml
    version: '3.7'
    
    x-airflow-common:
      &airflow-common
      build: .
      image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:2.9.2}
      environment:
        &airflow-common-env
        AIRFLOW__CORE__EXECUTOR: CeleryExecutor
        AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
        AIRFLOW__CELERY__RESULT_BACKEND: db+postgresql://airflow:airflow@postgres/airflow
        AIRFLOW__CELERY__BROKER_URL: redis://:@redis:6379/0
        AIRFLOW__CORE__FERNET_KEY: ''
        AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'true'
        AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
        AIRFLOW__API__AUTH_BACKENDS: 'airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session'
        AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK: 'true'
        _PIP_ADDITIONAL_REQUIREMENTS: ${_PIP_ADDITIONAL_REQUIREMENTS:-}
      volumes:
        - ${AIRFLOW_PROJ_DIR:-.}/dags:/opt/airflow/dags
        - ${AIRFLOW_PROJ_DIR:-.}/logs:/opt/airflow/logs
        - ${AIRFLOW_PROJ_DIR:-.}/config:/opt/airflow/config
        - ${AIRFLOW_PROJ_DIR:-.}/plugins:/opt/airflow/plugins
      user: "${AIRFLOW_UID:-50000}:0"
      depends_on:
        &airflow-common-depends-on
        redis:
          condition: service_healthy
        postgres:
          condition: service_healthy
  
  clickhouse:
    image: yandex/clickhouse-server:latest
    container_name: clickhouse_user
    ports:
      - "8123:8123"
      - "9000:9000"
    volumes:
      - clickhouse_data:/var/lib/clickhouse

5. **Запустите Docker Compose:**

   ```bash
    docker-compose up -d

6. **Запустите Docker Compose:**
- Перейдите в веб-интерфейс Airflow по адресу http://localhost:8080 и активируйте DAG main.

## Использование
- DAG загружает данные об объектах недвижимости из CSV файла, обрабатывает их экспортирует в формат Parquet и сохраняет в ClickHouse.
- После завершения обработки, DAG выполняет SQL-запрос для получения топ-25 домов площадью более 60 кв.м.
