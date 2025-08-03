from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import sqlite3
import os

# Configuração
TOKEN = os.getenv('GITHUB_TOKEN')
HEADERS = {
    "Authorization": f"token {TOKEN}",
    "Accept": "application/vnd.github+json"
}
LANGUAGES = ["Python", "C++", "C", "Java", "C#"]
DB = "/opt/airflow/db/github_repos.sqlite3"  # caminho no container do Airflow

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

# DAG Airflow
with DAG(
    dag_id='fetch_github_repos_hourly',
    default_args=default_args,
    start_date=datetime(2025, 8, 1),
    schedule_interval='@hourly',
    catchup=False
) as dag:

    def create_db():
        conn = sqlite3.connect(DB)
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS repos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                repo_id INTEGER UNIQUE,
                name TEXT,
                language TEXT,
                created_at TEXT
            )
        """)
        conn.commit()
        conn.close()

    def fetch_and_save():
        now = datetime.utcnow()
        one_hour_ago = now - timedelta(hours=1)
        created_range = f"{one_hour_ago.isoformat()}..{now.isoformat()}"

        conn = sqlite3.connect(DB)
        c = conn.cursor()

        for language in LANGUAGES:
            print(f"Fetching repositories for {language}")
            query = f"language:{language} created:{created_range}"
            params = {
                "q": query,
                "sort": "stars",
                "order": "desc",
                "per_page": 100
            }

            url = "https://api.github.com/search/repositories"
            response = requests.get(url, headers=HEADERS, params=params)
            if response.status_code != 200:
                print(f"Error fetching data: {response.status_code}")
                continue

            repos = response.json()["items"]
            for repo in repos:
                try:
                    c.execute("""
                        INSERT OR IGNORE INTO repos (repo_id, name, language, created_at)
                        VALUES (?, ?, ?, ?)
                    """, (repo["id"], repo["full_name"], language, repo["created_at"]))
                except sqlite3.Error as e:
                    print(f"DB error: {e}")

        conn.commit()
        conn.close()

    init_db = PythonOperator(
        task_id='create_db',
        python_callable=create_db
    )

    fetch_repos = PythonOperator(
        task_id='fetch_and_save',
        python_callable=fetch_and_save
    )

    init_db >> fetch_repos
