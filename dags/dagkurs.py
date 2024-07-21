from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
import re

def parse_kurs_kz():
    url = "https://kurs.kz"
    response = requests.get(url, verify=False)
    response.raise_for_status()  # Убедиться, что запрос успешен

    # Получаем текст страницы
    data = response.text

    # Регулярное выражение для поиска курсов валют
    kurs_pattern = re.compile(r'(\d{1,3}\.\d{1,2})')  # Подкорректированное регулярное выражение

    # фильтруем чтобы значения меньше 2 не показывалось. Потому что это явно не курс валюты ни для рубля ни для доллара.
    kurs_values = [float(match) for match in kurs_pattern.findall(data) if float(match) > 1]

    # Проверка, есть ли найденные курсы валют
    if kurs_values:
        # Находим минимальный и максимальный курс
        min_kurs = min(kurs_values)
        max_kurs = max(kurs_values)

        # Вывод результатов
        print(f"Минимальный курс: {min_kurs}")
        print(f"Максимальный курс: {max_kurs}")
    else:
        print("Курсы валют не найдены")

# Определение DAG
dag = DAG(
    dag_id='kurs_kz_dag',
    start_date=datetime(2024, 7, 19),
    schedule_interval='@daily',
    catchup=False,
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
)

# Определение задачи
task_parse_kurs_kz = PythonOperator(
    task_id='parse_kurs_kz',
    python_callable=parse_kurs_kz,
    dag=dag,
)

# Определение порядка выполнения задач (если есть несколько задач)
task_parse_kurs_kz
