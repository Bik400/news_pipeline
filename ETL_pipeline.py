# import libraries
import pandas as pd
import logging
import sqlite3
from newsapi import NewsApiClient
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, time

news_api_key = ""
news_api = NewsApiClient(api_key=news_api_key)

# set datetime variables for airflow
to_date = datetime.utcnow().date()
from_date = to_date - timedelta(days=1)

# initialize DAG object
dag = DAG(
    dag_id = "News_ETL",
    default_args={'start_date':datetime.combine(from_date, time(0,0)), 'retries': 1},
    schedule_interval='@daily'
)

# Extract data from news api
def extract_news_data(**kwargs):
    try:
        news = news_api.get_everything(q="AI", language="en", from_param=from_date, to=to_date)
        logging.info("Connection is successful")
        kwargs['task_instance'].xcom_push(key='extract_result', value=news['articles'])
    except:
        logging.error("Connection unsuccessful")

# Transform the data 
def clean_author_col(text):
    try:
        return text.split(',')[0].title()
    except AttributeError:
        return "No Author"

def transform_news_data(**kwargs):
    article_list = []
    articles = kwargs['task_instance'].xcom_pull(task_ids='extract_news', key='extract_result')
    logging.info("Data pulled successfully.")
    for article in articles:
        article_list.append([article["author"],article["title"],article["publishedAt"],article["content"],article["url"],article["source"]["name"]])
    articles_df = pd.DataFrame(article_list, columns=["Author", "Title", "PublishedAt", "Content", "URL", "Source"])
    articles_df["PublishedAt"] = pd.to_datetime(articles_df["PublishedAt"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    articles_df["Author"] = articles_df["Author"].apply(clean_author_col)
    kwargs['task_instance'].xcom_push(key='transform_df', value=articles_df.to_json())
    logging.info("Transformed data pushed to XCom successfully.")

# Load the data into a sqlite db
def load_news_data(**kwargs):
    con = sqlite3.connect("NewsDB")
    cur = con.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS news_table(
    "Source" VARCHAR(30), 
    "Author" TEXT, 
    "News Title" TEXT, 
    "URL" TEXT, 
    "Date Published" TEXT, 
    "Content" TEXT
    )
    ''')
    data = kwargs['task_instance'].xcom_pull(task_ids='transform_news', key='transform_df')
    df = pd.read_json(data)
    logging.info("Ready to load data to DB.")
    df.to_sql(name="news_table", con=con, index=False, if_exists="append")
    logging.info("Successfully loaded data to DB.")


# create python operators
_extract_news_data = PythonOperator(
    task_id="extract_news",
    python_callable = extract_news_data,
    provide_context = True,
    dag = dag
)

_transform_news_data = PythonOperator(
    task_id = "transform_news",
    python_callable = transform_news_data,
    provide_context = True,
    dag = dag
)

_load_news_data = PythonOperator(
    task_id = "load_news",
    python_callable = load_news_data,
    provide_context = True,
    dag = dag
)

# create dependencies
_extract_news_data >> _transform_news_data >> _load_news_data
    