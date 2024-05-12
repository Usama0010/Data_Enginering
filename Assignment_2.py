import requests
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import csv
import re

sources = ['https://www.dawn.com/', 'https://www.bbc.com/']

def extract(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    articles = []
    for article in soup.find_all('article'):
        title_element = article.find('h2')
        desc_element = article.find('p')
        if title_element and desc_element:  # Check if both title and description elements exist
            title = title_element.text.strip()
            desc = desc_element.text.strip()
            articles.append((title, desc))
    return articles

def transform(**kwargs):
    ti = kwargs['ti']
    extracted_data = ti.xcom_pull(task_ids=f"extract_{kwargs['source'].split('.')[1]}")
    transformed_data = [(clean_text(url),) for url in extracted_data]  # Cleaning each URL
    return transformed_data

def clean_text(text):
    text = re.sub(r'\s+', ' ', text)  # Remove extra whitespaces
    text = re.sub(r'[^\w\s]', '', text)  # Remove punctuation
    return text.strip()

def save_to_csv(data, filename):
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Cleaned URL'])  # Changed column header to reflect cleaned data
        writer.writerows(data)

# Fetch data from Dawn and BBC
dawn_articles = extract('https://www.dawn.com')
bbc_articles = extract('https://www.bbc.com')
# Clean the data
cleaned_data = [(clean_text(title), clean_text(desc)) for title, desc in dawn_articles]

# Save cleaned data to CSV
save_to_csv(cleaned_data, 'dawn_articles.csv')

default_args = {
    'owner': 'airflow-demo'
}

dag = DAG(
    'mlops-dag',
    default_args=default_args,
    description='A simple DAG to perform ETL tasks',
    schedule_interval=None,
)

for source in sources:
    extract_task = PythonOperator(
        task_id=f"extract_{source.split('.')[1]}",
        python_callable=extract,
        op_kwargs={'url': source},
        dag=dag,
    )

    transform_task = PythonOperator(
        task_id=f"transform_{source.split('.')[1]}",
        python_callable=transform,
        op_kwargs={'source': source},
        provide_context=True,
        dag=dag,
    )

    load_task = PythonOperator(
        task_id=f"load_{source.split('.')[1]}",
        python_callable=save_to_csv,
        op_kwargs={'filename': f"{source.split('.')[1]}_extracted_data.csv"},
        provide_context=True,
        dag=dag,
    )

    extract_task >> transform_task >> load_task
