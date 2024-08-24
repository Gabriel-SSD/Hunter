import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

def generate_date_range(start_date, end_date):
    dates = pd.date_range(start=start_date, end=end_date)
    return dates

def create_dim_time_df(start_date, end_date):
    dates = generate_date_range(start_date, end_date)
    data = {
        'id': dates.strftime('%Y%m%d').astype(int),
        'date': dates,
        'year': dates.year,
        'quarter': dates.quarter,
        'month': dates.month,
        'day_of_month': dates.day,
        'abbr_day_name': dates.strftime('%a'),
        'day_name': dates.strftime('%A'),  # Name of the day in English
        'day_of_week': dates.weekday + 1,  # 1 = Monday, 7 = Sunday
        'is_weekend': dates.weekday >= 5  # Saturday and Sunday
    }
    df = pd.DataFrame(data)
    return df

def save_to_postgres(df, table_name):
    load_dotenv()
    database_url = os.getenv('DATABASE_URL')
    engine = create_engine(database_url)
    df.to_sql(table_name, engine, if_exists='append', index=False)

start_date = '2024-01-01'
end_date = '2030-12-31'
dim_time_df = create_dim_time_df(start_date, end_date)
save_to_postgres(dim_time_df, 'd_time')
