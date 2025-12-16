from typing import List
from sqlalchemy import select
from database.session import SessionLocal
from database.Models import Company, PriceHistory, ETLRun
import datetime

def get_session():
    return SessionLocal()

def ensure_companies(session, symbols: List[dict]):
    """
    Accepts list of dicts: [{"symbol": "RELIANCE.NS", "name": "Reliance", "exchange": "NSE"}, ...]
    Inserts missing companies into companies table and returns mapping symbol -> company_id
    """
    mapping = {}
    for s in symbols:
        sym = s["symbol"]
        stmt = select(Company).where(Company.symbol == sym, Company.exchange == s.get("exchange", "NSE"))
        res = session.execute(stmt).scalar_one_or_none()
        if res is None:
            c = Company(symbol=sym, name=s.get("name"), exchange=s.get("exchange", "NSE"))
            session.add(c)
            session.flush()  # get id
            mapping[sym] = c.id
        else:
            mapping[sym] = res.id
    return mapping

def get_existing_dates(session, company_id: int):
    """Return set of trade_date strings already present for company_id"""
    stmt = select(PriceHistory.trade_date).where(PriceHistory.company_id == company_id)
    res = session.execute(stmt).scalars().all()
    return set([d.isoformat() for d in res])

def log_etl_run(session, pipeline_name: str, status: str, rows_processed: int = 0, error_message: str = None, started_at=None, ended_at=None):
    er = ETLRun(
        pipeline_name=pipeline_name,
        status=status,
        rows_processed=rows_processed,
        error_message=error_message,
        started_at=started_at,
        ended_at=ended_at,
    )
    session.add(er)
    session.commit()
    return er.id
