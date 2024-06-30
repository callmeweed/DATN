from sqlalchemy import create_engine
import os
from pathlib import Path
from dotenv import load_dotenv
env_path = Path(".") / "/Users/hieunguyen/DATN/.env"
load_dotenv(dotenv_path=env_path)

POSTGRES_USER = os.environ.get("POSTGRES_USER")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD")
POSTGRES_HOST = os.environ.get("POSTGRES_HOST")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT")
POSTGRES_DB = os.environ.get("POSTGRES_DB")


def get_postgres_engine():
    engine = create_engine(
        f'postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}')
    return engine

