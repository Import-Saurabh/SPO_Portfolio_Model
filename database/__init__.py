# database/__init__.py
from .session import SessionLocal, engine, Base
from .Models import *  # noqa: F401,F403

# Optional small helper to create all tables from models (useful in dev)
def create_all():
    Base.metadata.create_all(bind=engine)
