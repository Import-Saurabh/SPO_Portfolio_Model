-- Final production schema for SPO system
-- Default DB: spo_db
SET FOREIGN_KEY_CHECKS=0;
DROP TABLE IF EXISTS events;
DROP TABLE IF EXISTS model_versions;
DROP TABLE IF EXISTS exchange_holidays;
DROP TABLE IF EXISTS financial_ratios;
DROP TABLE IF EXISTS financials_cashflow_statement;
DROP TABLE IF EXISTS financials_income_statement;
DROP TABLE IF EXISTS financials_balance_sheet;
DROP TABLE IF EXISTS etl_runs;
DROP TABLE IF EXISTS backtest_results;
DROP TABLE IF EXISTS optimized_portfolios;
DROP TABLE IF EXISTS covariance_matrices;
DROP TABLE IF EXISTS model_predictions;
DROP TABLE IF EXISTS features_daily;
DROP TABLE IF EXISTS price_history;
DROP TABLE IF EXISTS corporate_actions;
DROP TABLE IF EXISTS companies;
SET FOREIGN_KEY_CHECKS=1;

CREATE DATABASE IF NOT EXISTS `spo_db` CHARACTER SET = 'utf8mb4' COLLATE = 'utf8mb4_unicode_ci';
USE `spo_db`;

-- 1. companies
CREATE TABLE IF NOT EXISTS companies (
    id INT PRIMARY KEY AUTO_INCREMENT,
    symbol VARCHAR(32) NOT NULL,
    name VARCHAR(128),
    exchange VARCHAR(32) NOT NULL,
    sector VARCHAR(64),
    industry VARCHAR(128),
    isin VARCHAR(32) UNIQUE,
    listing_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_symbol_exchange (symbol, exchange)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 2. corporate_actions
CREATE TABLE IF NOT EXISTS corporate_actions (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    company_id INT NOT NULL,
    action_type ENUM('DIVIDEND','SPLIT','BONUS','RIGHTS') NOT NULL,
    action_date DATE NOT NULL,
    value DECIMAL(24,6),
    ratio_from INT,
    ratio_to INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_ca_company FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 3. price_history
CREATE TABLE IF NOT EXISTS price_history (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    company_id INT NOT NULL,
    trade_date DATE NOT NULL,
    open DECIMAL(24,6),
    high DECIMAL(24,6),
    low DECIMAL(24,6),
    close DECIMAL(24,6),
    adj_close DECIMAL(24,6),
    volume BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_price_company_date (company_id, trade_date),
    KEY idx_price_company_date (company_id, trade_date),
    CONSTRAINT fk_price_company FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 4. features_daily
CREATE TABLE IF NOT EXISTS features_daily (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    company_id INT NOT NULL,
    feature_date DATE NOT NULL,
    return_1d DECIMAL(24,6),
    return_5d DECIMAL(24,6),
    return_10d DECIMAL(24,6),
    return_21d DECIMAL(24,6),
    volatility_10d DECIMAL(24,6),
    volatility_20d DECIMAL(24,6),
    volatility_60d DECIMAL(24,6),
    momentum_14d DECIMAL(24,6),
    volume_change_5d DECIMAL(24,6),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_features_company_date (company_id, feature_date),
    CONSTRAINT fk_features_company FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 5. model_predictions
CREATE TABLE IF NOT EXISTS model_predictions (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    company_id INT NOT NULL,
    prediction_date DATE NOT NULL,
    predicted_return DECIMAL(24,6),
    model_version VARCHAR(128) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_pred_company_date_version (company_id, prediction_date, model_version),
    CONSTRAINT fk_pred_company FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 6. covariance_matrices
CREATE TABLE IF NOT EXISTS covariance_matrices (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    calc_date DATE NOT NULL,
    num_assets INT NOT NULL,
    matrix_json LONGTEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_cov_calc_date (calc_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 7. optimized_portfolios
CREATE TABLE IF NOT EXISTS optimized_portfolios (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    portfolio_date DATE NOT NULL,
    weights_json LONGTEXT NOT NULL,
    objective_value DECIMAL(24,6),
    model_version VARCHAR(128) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_portfolio_date_version (portfolio_date, model_version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 8. backtest_results
CREATE TABLE IF NOT EXISTS backtest_results (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    run_id VARCHAR(128) NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    sharpe DECIMAL(24,6),
    max_drawdown DECIMAL(24,6),
    total_return DECIMAL(24,6),
    parameters_json LONGTEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 9. etl_runs
CREATE TABLE IF NOT EXISTS etl_runs (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    pipeline_name VARCHAR(256) NOT NULL,
    status ENUM('SUCCESS','FAILED','PARTIAL') NOT NULL,
    rows_processed INT,
    error_message TEXT,
    started_at TIMESTAMP NULL,
    ended_at TIMESTAMP NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 10. financials_balance_sheet
CREATE TABLE IF NOT EXISTS financials_balance_sheet (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    company_id INT NOT NULL,
    fiscal_year INT NOT NULL,
    fiscal_quarter TINYINT NOT NULL,
    report_date DATE NOT NULL,
    total_assets DECIMAL(30,2),
    total_liabilities DECIMAL(30,2),
    shareholder_equity DECIMAL(30,2),
    current_assets DECIMAL(30,2),
    current_liabilities DECIMAL(30,2),
    cash_and_equivalents DECIMAL(30,2),
    inventory DECIMAL(30,2),
    receivables DECIMAL(30,2),
    long_term_debt DECIMAL(30,2),
    short_term_debt DECIMAL(30,2),
    retained_earnings DECIMAL(30,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_bs_company_fy_fq (company_id, fiscal_year, fiscal_quarter),
    CONSTRAINT fk_bs_company FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 11. financials_income_statement
CREATE TABLE IF NOT EXISTS financials_income_statement (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    company_id INT NOT NULL,
    fiscal_year INT NOT NULL,
    fiscal_quarter TINYINT NOT NULL,
    report_date DATE NOT NULL,
    revenue DECIMAL(30,2),
    cost_of_revenue DECIMAL(30,2),
    gross_profit DECIMAL(30,2),
    operating_expenses DECIMAL(30,2),
    operating_income DECIMAL(30,2),
    interest_expense DECIMAL(30,2),
    pretax_income DECIMAL(30,2),
    net_income DECIMAL(30,2),
    ebit DECIMAL(30,2),
    ebitda DECIMAL(30,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_is_company_fy_fq (company_id, fiscal_year, fiscal_quarter),
    CONSTRAINT fk_is_company FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 12. financials_cashflow_statement
CREATE TABLE IF NOT EXISTS financials_cashflow_statement (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    company_id INT NOT NULL,
    fiscal_year INT NOT NULL,
    fiscal_quarter TINYINT NOT NULL,
    report_date DATE NOT NULL,
    operating_cash_flow DECIMAL(30,2),
    investing_cash_flow DECIMAL(30,2),
    financing_cash_flow DECIMAL(30,2),
    capital_expenditure DECIMAL(30,2),
    free_cash_flow DECIMAL(30,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_cf_company_fy_fq (company_id, fiscal_year, fiscal_quarter),
    CONSTRAINT fk_cf_company FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 13. financial_ratios
CREATE TABLE IF NOT EXISTS financial_ratios (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    company_id INT NOT NULL,
    fiscal_year INT NOT NULL,
    fiscal_quarter TINYINT NOT NULL,
    report_date DATE NOT NULL,
    pe_ratio DECIMAL(24,8),
    pb_ratio DECIMAL(24,8),
    roe DECIMAL(24,8),
    roa DECIMAL(24,8),
    debt_to_equity DECIMAL(24,8),
    current_ratio DECIMAL(24,8),
    quick_ratio DECIMAL(24,8),
    ebitda_margin DECIMAL(24,8),
    net_margin DECIMAL(24,8),
    fcf_yield DECIMAL(24,8),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_ratios_company_fy_fq (company_id, fiscal_year, fiscal_quarter),
    CONSTRAINT fk_ratios_company FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- OPTIONAL: 14. exchange_holidays
CREATE TABLE IF NOT EXISTS exchange_holidays (
    id INT PRIMARY KEY AUTO_INCREMENT,
    exchange VARCHAR(64) NOT NULL,
    holiday_date DATE NOT NULL,
    description VARCHAR(256),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_exchange_holiday (exchange, holiday_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- OPTIONAL: 15. model_versions
CREATE TABLE IF NOT EXISTS model_versions (
    id INT PRIMARY KEY AUTO_INCREMENT,
    version VARCHAR(128) NOT NULL UNIQUE,
    model_type VARCHAR(64) NOT NULL,
    train_start DATE,
    train_end DATE,
    hyperparams_json LONGTEXT,
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- OPTIONAL: 16. events / news_sentiment
CREATE TABLE IF NOT EXISTS events (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    company_id INT,
    event_date DATE NOT NULL,
    event_type VARCHAR(128),
    event_source VARCHAR(256),
    headline TEXT,
    sentiment_score DECIMAL(10,6),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_events_company FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Indexes for performance (examples)
CREATE INDEX idx_price_trade_date ON price_history (trade_date);
CREATE INDEX idx_features_date ON features_daily (feature_date);
CREATE INDEX idx_pred_date ON model_predictions (prediction_date);
CREATE INDEX idx_cov_date ON covariance_matrices (calc_date);
CREATE INDEX idx_portfolio_date ON optimized_portfolios (portfolio_date);
