import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv


def generate_date_range(start_date, end_date):
    dates = pd.date_range(start=start_date, end=end_date, freq='h')
    return dates


def get_day_period(hour):
    if 6 <= hour < 12:
        return 'morning'
    elif 12 <= hour < 18:
        return 'afternoon'
    elif 18 <= hour < 21:
        return 'evening'
    else:
        return 'night'


def create_dim_time_df(start_date, end_date):
    dates = generate_date_range(start_date, end_date)

    # Gerar o id como um inteiro no formato YYYYMMDDHH
    ids = [int(date.strftime('%Y%m%d') + f'{date.hour:02d}') for date in dates]

    data = {
        'id': ids,  # Usando o novo formato de id
        'date': dates,
        'year': dates.year,
        'quarter': dates.quarter,
        'month': dates.month,
        'day_of_month': dates.day,
        'abbr_day_name': dates.strftime('%a'),
        'day_name': dates.strftime('%A'),
        'day_of_week': dates.weekday + 1,  # Para garantir que a semana comece em 1 (segunda-feira)
        'is_weekend': dates.weekday >= 5,  # Fim de semana é sábado e domingo
        'hour_of_day': dates.hour,  # Horário da hora (0-23)
        'day_period': [get_day_period(hour) for hour in dates.hour]  # Período do dia
    }

    df = pd.DataFrame(data)
    return df


def save_to_postgres(df, table_name):
    load_dotenv()
    database_url = os.getenv('DATABASE_URL')
    engine = create_engine(database_url)
    df.to_sql(table_name, engine, if_exists='replace', index=False)


start_date = '2024-01-01'
end_date = '2026-12-31'
dim_time_df = create_dim_time_df(start_date, end_date)
save_to_postgres(dim_time_df, 'd_time2')
