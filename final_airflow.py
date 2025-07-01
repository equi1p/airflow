import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

default_args = {
    'owner': 'k.marin',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2021, 10, 7)
}

link = "https://kc-course-static.hb.ru-msk.vkcs.cloud/startda/Video%20Game%20Sales.csv"
year = 1994 + hash(f'kirill-marin-jjr8777') % 23


@dag(default_args=default_args, catchup=False, schedule_interval = '0 15 * * *')
def k_marin_dag():
    
    @task(retries = 3)
    def get_data():
        data = pd.read_csv(link)
        return data.to_csv(index=False)
    
    @task(retries = 3)
    def get_most_sales_game(data):
        df = pd.read_csv(StringIO(data))
        most_sales_game = df.query('Year == @year').sort_values(by='Global_Sales', ascending=False).head(1).Name.values.item()
        return most_sales_game
    
    @task(retries = 3)
    def get_EU_genre(data):
        df = pd.read_csv(StringIO(data))
        EU_genre = df.query('Year == @year').groupby('Genre', as_index=False).agg({'EU_Sales':'sum'}).sort_values(by='EU_Sales', ascending=False).head(1).Genre.values.item()
        return EU_genre
    
    @task(retries = 3)
    def get_most_sales_platform(data):
        df = pd.read_csv(StringIO(data))
        most_sales_platform = df.query('Year == @year & NA_Sales > 1').groupby('Platform', as_index=False).agg({'Name':'count'}).sort_values(by='Name', ascending=False).head(1).Platform.values.item()
        return most_sales_platform
    
    @task(retries = 3)
    def get_avg_sales_publisher(data):
        df = pd.read_csv(StringIO(data))
        avg_sales_publisher = df.query('Year == @year').groupby('Publisher', as_index=False).agg({'JP_Sales':'mean'}) \
    .sort_values('JP_Sales', ascending = False).head(1).Publisher.values.item()
        return avg_sales_publisher
    
    @task(retries = 3)
    def get_count_games(data):
        df = pd.read_csv(StringIO(data))
        count_games = df.query('Year == @year & EU_Sales > JP_Sales').Name.count().item()
        return count_games
    
    @task(retries = 3)
    def print_data(t1, t2, t3, t4, t5):
        context = get_current_context()
        date = context['ds']
        
        print(f'Самая продаваемая игра в 2015 году: {t1}.')
        print(f'Самые продаваемые игры в Европе в 2015 году были жанра: {t2}.')
        print(f'Больше всего игр, которые продались более чем миллионным тиражом в Северной Америке, были на платформе: {t3}.')
        print(f'Самые высокие продажи в Японии у издателя: {t4}.')
        print(f'Сколько игр продались больше в Европе лучше в Японии: {t5}')
        
    data = get_data()
    most_sales_game = get_most_sales_game(data)
    EU_genre = get_EU_genre(data)
    most_sales_platform = get_most_sales_platform(data)
    avg_sales_publisher = get_avg_sales_publisher(data)
    count_games = get_count_games(data)
    print_data(most_sales_game, EU_genre, most_sales_platform, avg_sales_publisher, count_games)
    
k_marin_dag = k_marin_dag() 