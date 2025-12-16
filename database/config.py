# database/config.py
# DB connection configuration used by SQLAlchemy session and Alembic.
# Adjust via environment variables or leave defaults (matches docker-compose defaults).

import os

DB_USER = os.getenv("MYSQL_USER", "spo_user")
DB_PASS = os.getenv("MYSQL_PASSWORD", "spo_pass")
DB_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
DB_PORT = os.getenv("MYSQL_PORT", "3306")
DB_NAME = os.getenv("MYSQL_DATABASE", "spo_db")

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4",
)

