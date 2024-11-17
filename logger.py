import logging
import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, DateTime
from sqlalchemy.orm import sessionmaker
from datetime import datetime


class DatabaseHandler(logging.Handler):
    def __init__(self, db_url, table_name='logs'):
        super().__init__()
        self.engine = create_engine(db_url)
        self.Session = sessionmaker(bind=self.engine)

        # Definindo a tabela de logs
        self.metadata = MetaData()
        self.logs_table = Table(
            table_name, self.metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('timestamp', DateTime, nullable=False),
            Column('level', String(50), nullable=False),
            Column('message', String, nullable=False)
        )

        # Criação da tabela, se ela ainda não existir
        self.metadata.create_all(self.engine)

    def emit(self, record):
        session = self.Session()
        try:
            log_entry = self.format(record)
            timestamp = datetime.now()

            insert_stmt = self.logs_table.insert().values(
                timestamp=timestamp,
                level=record.levelname,
                message=log_entry
            )
            session.execute(insert_stmt)
            session.commit()
        except Exception as e:
            print(f"Erro ao gravar log no banco de dados: {e}")
            session.rollback()
        finally:
            session.close()

    def close(self):
        super().close()


def setup_logging():
    load_dotenv(dotenv_path='.env')
    db_url = os.getenv('DATABASE_URL')

    logger = logging.getLogger('hunter')

    # Verifique se já existe um *handler* para evitar duplicação
    if not any(isinstance(handler, DatabaseHandler) for handler in logger.handlers):
        logger.setLevel(logging.INFO)

        db_handler = DatabaseHandler(db_url)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        db_handler.setFormatter(formatter)

        logger.addHandler(db_handler)

    return logger