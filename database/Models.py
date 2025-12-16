from sqlalchemy import (
    Column,
    Integer,
    BigInteger,
    String,
    Date,
    DateTime,
    Enum,
    Numeric,
    Text,
    ForeignKey,
    UniqueConstraint,
    Index,
    JSON,
)
from sqlalchemy.orm import relationship
from .session import Base
import datetime

# Numeric precision helper: use Numeric(24,6) or Numeric(30,2) matching schema
N_PRICE = Numeric(24, 6)
N_BIG = Numeric(30, 2)
N_RATIO = Numeric(24, 8)


class Company(Base):
    __tablename__ = "companies"
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(32), nullable=False)
    name = Column(String(128))
    exchange = Column(String(32), nullable=False)
    sector = Column(String(64))
    industry = Column(String(128))
    isin = Column(String(32), unique=True)
    listing_date = Column(Date)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

    __table_args__ = (UniqueConstraint("symbol", "exchange", name="uq_symbol_exchange"),)

    price_history = relationship("PriceHistory", back_populates="company", cascade="all, delete-orphan")
    corporate_actions = relationship("CorporateAction", back_populates="company", cascade="all, delete-orphan")
    features = relationship("FeatureDaily", back_populates="company", cascade="all, delete-orphan")
    predictions = relationship("ModelPrediction", back_populates="company", cascade="all, delete-orphan")


class CorporateAction(Base):
    __tablename__ = "corporate_actions"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey("companies.id", ondelete="CASCADE"), nullable=False)
    action_type = Column(Enum("DIVIDEND", "SPLIT", "BONUS", "RIGHTS", name="action_type_enum"), nullable=False)
    action_date = Column(Date, nullable=False)
    value = Column(N_PRICE)
    ratio_from = Column(Integer)
    ratio_to = Column(Integer)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

    company = relationship("Company", back_populates="corporate_actions")


class PriceHistory(Base):
    __tablename__ = "price_history"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey("companies.id", ondelete="CASCADE"), nullable=False)
    trade_date = Column(Date, nullable=False)
    open = Column(N_PRICE)
    high = Column(N_PRICE)
    low = Column(N_PRICE)
    close = Column(N_PRICE)
    adj_close = Column(N_PRICE)
    volume = Column(BigInteger)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

    company = relationship("Company", back_populates="price_history")

    __table_args__ = (
        UniqueConstraint("company_id", "trade_date", name="uq_price_company_date"),
        Index("idx_price_company_date", "company_id", "trade_date"),
    )


class FeatureDaily(Base):
    __tablename__ = "features_daily"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey("companies.id", ondelete="CASCADE"), nullable=False)
    feature_date = Column(Date, nullable=False)

    return_1d = Column(N_PRICE)
    return_5d = Column(N_PRICE)
    return_10d = Column(N_PRICE)
    return_21d = Column(N_PRICE)

    volatility_10d = Column(N_PRICE)
    volatility_20d = Column(N_PRICE)
    volatility_60d = Column(N_PRICE)

    momentum_14d = Column(N_PRICE)
    volume_change_5d = Column(N_PRICE)

    created_at = Column(DateTime, default=datetime.datetime.utcnow)

    company = relationship("Company", back_populates="features")

    __table_args__ = (UniqueConstraint("company_id", "feature_date", name="uq_features_company_date"),)


class ModelPrediction(Base):
    __tablename__ = "model_predictions"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey("companies.id", ondelete="CASCADE"), nullable=False)
    prediction_date = Column(Date, nullable=False)
    predicted_return = Column(N_PRICE)
    model_version = Column(String(128), nullable=False)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

    company = relationship("Company", back_populates="predictions")

    __table_args__ = (
        UniqueConstraint("company_id", "prediction_date", "model_version", name="uq_pred_company_date_version"),
    )


class CovarianceMatrix(Base):
    __tablename__ = "covariance_matrices"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    calc_date = Column(Date, nullable=False)
    num_assets = Column(Integer, nullable=False)
    matrix_json = Column(Text, nullable=False)  # JSON string, optionally compressed
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

    __table_args__ = (UniqueConstraint("calc_date", name="uq_cov_calc_date"),)


class OptimizedPortfolio(Base):
    __tablename__ = "optimized_portfolios"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    portfolio_date = Column(Date, nullable=False)
    weights_json = Column(Text, nullable=False)
    objective_value = Column(N_PRICE)
    model_version = Column(String(128), nullable=False)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

    __table_args__ = (UniqueConstraint("portfolio_date", "model_version", name="uq_portfolio_date_version"),)


class BacktestResult(Base):
    __tablename__ = "backtest_results"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    run_id = Column(String(128), nullable=False)
    start_date = Column(Date, nullable=False)
    end_date = Column(Date, nullable=False)
    sharpe = Column(N_PRICE)
    max_drawdown = Column(N_PRICE)
    total_return = Column(N_PRICE)
    parameters_json = Column(Text)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)


class ETLRun(Base):
    __tablename__ = "etl_runs"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    pipeline_name = Column(String(256), nullable=False)
    status = Column(Enum("SUCCESS", "FAILED", "PARTIAL", name="etl_status_enum"), nullable=False)
    rows_processed = Column(Integer)
    error_message = Column(Text)
    started_at = Column(DateTime)
    ended_at = Column(DateTime)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)


# Fundamentals tables
class FinancialsBalanceSheet(Base):
    __tablename__ = "financials_balance_sheet"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey("companies.id", ondelete="CASCADE"), nullable=False)
    fiscal_year = Column(Integer, nullable=False)
    fiscal_quarter = Column(Integer, nullable=False)
    report_date = Column(Date, nullable=False)

    total_assets = Column(N_BIG)
    total_liabilities = Column(N_BIG)
    shareholder_equity = Column(N_BIG)
    current_assets = Column(N_BIG)
    current_liabilities = Column(N_BIG)
    cash_and_equivalents = Column(N_BIG)
    inventory = Column(N_BIG)
    receivables = Column(N_BIG)
    long_term_debt = Column(N_BIG)
    short_term_debt = Column(N_BIG)
    retained_earnings = Column(N_BIG)

    created_at = Column(DateTime, default=datetime.datetime.utcnow)

    __table_args__ = (UniqueConstraint("company_id", "fiscal_year", "fiscal_quarter", name="uq_bs_company_fy_fq"),)


class FinancialsIncomeStatement(Base):
    __tablename__ = "financials_income_statement"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey("companies.id", ondelete="CASCADE"), nullable=False)
    fiscal_year = Column(Integer, nullable=False)
    fiscal_quarter = Column(Integer, nullable=False)
    report_date = Column(Date, nullable=False)

    revenue = Column(N_BIG)
    cost_of_revenue = Column(N_BIG)
    gross_profit = Column(N_BIG)
    operating_expenses = Column(N_BIG)
    operating_income = Column(N_BIG)
    interest_expense = Column(N_BIG)
    pretax_income = Column(N_BIG)
    net_income = Column(N_BIG)
    ebit = Column(N_BIG)
    ebitda = Column(N_BIG)

    created_at = Column(DateTime, default=datetime.datetime.utcnow)

    __table_args__ = (UniqueConstraint("company_id", "fiscal_year", "fiscal_quarter", name="uq_is_company_fy_fq"),)


class FinancialsCashflow(Base):
    __tablename__ = "financials_cashflow_statement"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey("companies.id", ondelete="CASCADE"), nullable=False)
    fiscal_year = Column(Integer, nullable=False)
    fiscal_quarter = Column(Integer, nullable=False)
    report_date = Column(Date, nullable=False)

    operating_cash_flow = Column(N_BIG)
    investing_cash_flow = Column(N_BIG)
    financing_cash_flow = Column(N_BIG)
    capital_expenditure = Column(N_BIG)
    free_cash_flow = Column(N_BIG)

    created_at = Column(DateTime, default=datetime.datetime.utcnow)

    __table_args__ = (UniqueConstraint("company_id", "fiscal_year", "fiscal_quarter", name="uq_cf_company_fy_fq"),)


class FinancialRatio(Base):
    __tablename__ = "financial_ratios"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey("companies.id", ondelete="CASCADE"), nullable=False)
    fiscal_year = Column(Integer, nullable=False)
    fiscal_quarter = Column(Integer, nullable=False)
    report_date = Column(Date, nullable=False)

    pe_ratio = Column(N_RATIO)
    pb_ratio = Column(N_RATIO)
    roe = Column(N_RATIO)
    roa = Column(N_RATIO)
    debt_to_equity = Column(N_RATIO)
    current_ratio = Column(N_RATIO)
    quick_ratio = Column(N_RATIO)
    ebitda_margin = Column(N_RATIO)
    net_margin = Column(N_RATIO)
    fcf_yield = Column(N_RATIO)

    created_at = Column(DateTime, default=datetime.datetime.utcnow)

    __table_args__ = (UniqueConstraint("company_id", "fiscal_year", "fiscal_quarter", name="uq_ratios_company_fy_fq"),)


# Optional enterprise tables
class ExchangeHoliday(Base):
    __tablename__ = "exchange_holidays"
    id = Column(Integer, primary_key=True, autoincrement=True)
    exchange = Column(String(64), nullable=False)
    holiday_date = Column(Date, nullable=False)
    description = Column(String(256))
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

    __table_args__ = (UniqueConstraint("exchange", "holiday_date", name="uq_exchange_holiday"),)


class ModelVersion(Base):
    __tablename__ = "model_versions"
    id = Column(Integer, primary_key=True, autoincrement=True)
    version = Column(String(128), nullable=False, unique=True)
    model_type = Column(String(64), nullable=False)
    train_start = Column(Date)
    train_end = Column(Date)
    hyperparams_json = Column(Text)
    notes = Column(Text)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)


class Event(Base):
    __tablename__ = "events"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey("companies.id", ondelete="SET NULL"), nullable=True)
    event_date = Column(Date, nullable=False)
    event_type = Column(String(128))
    event_source = Column(String(256))
    headline = Column(Text)
    sentiment_score = Column(N_RATIO)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

    