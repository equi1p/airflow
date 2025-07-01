import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'https://storage.yandexcloud.net/kc-startda/top-1m.csv'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


def get_data():
    # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


def get_top_10_url():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['url'] = top_data_df['domain'].str.split('.').str[-1]
    top_10_url = top_data_df.groupby('url', as_index=False).agg({'domain':'count'}).sort_values(by='domain', ascending=False).head(10)
    
    with open('top_10_url.csv', 'w') as f:
        f.write(top_10_url.to_csv(index=False, header=False))


def get_most_len_domain():
    data = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    data['len_name'] = data['domain'].str.len()
    most_len_domain = data.sort_values(by = 'domain', ascending=False).sort_values(by = 'len_name', ascending=False).head(1)
    with open('most_len_domain.csv', 'w') as f:
        f.write(most_len_domain.to_csv(index=False, header=False))

        
def get_airflow():
    data = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    data['len_name'] = data['domain'].str.len()
    airflow = data.sort_values(by = 'domain',ascending=False).sort_values(by='len_name', ascending=False, ignore_index=True).query('domain == "airflow.com"')
 
    with open('airflow.csv', 'w') as f:
        f.write(airflow.to_csv(index=False, header=False))
        

def print_data(ds):
    with open('top_10_url.csv', 'r') as f:
        top_10_url = f.read()
    with open('most_len_domain.csv', 'r') as f:
        most_len_domain = f.read()
    with open('airflow.csv', 'r') as f:
        airflow = f.read() 
    date = ds

    print(f'Top 10 url on {date}')
    print(top_10_url)

    print(f'The most length domain on {date}')
    print(most_len_domain)
    
    print(f'The place of airflow on {date}')
    print(airflow)


default_args = {
    'owner': 'k.marin',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 6, 2),
}
schedule_interval = '30 13 * * *'

dag = DAG('kirill_marin', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_url = PythonOperator(task_id='get_top_10_url',
                    python_callable=get_top_10_url,
                    dag=dag)

t2_len = PythonOperator(task_id='get_most_len_domain',
                        python_callable=get_most_len_domain,
                        dag=dag)

t2_airflow = PythonOperator(task_id='get_airflow',
                        python_callable=get_airflow,
                        dag=dag)


t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2_url, t2_len, t2_airflow] >> t3

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)