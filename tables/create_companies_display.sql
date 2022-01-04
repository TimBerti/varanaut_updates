CREATE TABLE companies_display (
    ticker VARCHAR(32),
    currency VARCHAR(32),
    name VARCHAR(255),
    time TIMESTAMP,
    ipo TIMESTAMP,
    country VARCHAR(255),
    industry VARCHAR(255),
    sector VARCHAR(255),
    website VARCHAR(255),
    isin VARCHAR(255),
    fiscal_year_end VARCHAR(255),
    gic_sector VARCHAR(255),
    gic_group VARCHAR(255),
    gic_industry VARCHAR(255),
    gic_sub_industry VARCHAR(255),
    is_delisted BOOLEAN,
    address VARCHAR(255),
    full_time_employees BIGINT,
    description TEXT,
    beta FLOAT8,
    avg_rating_analyst FLOAT8,
    target_price_analyst FLOAT8,
    ratings_analyst jsonb,
    short_interest_ratio FLOAT8,
    short_ratio_float FLOAT8,
    short_ratio_outstanding FLOAT8,
    shares_short FLOAT8,
    shares_short_prior_month FLOAT8,
    shares_held_by_insiders FLOAT8,
    shares_held_by_institutions FLOAT8,
    shares_float FLOAT8,
    outstanding_shares FLOAT8,
    outstanding_shares_change FLOAT8,
    total_revenue FLOAT8,
    free_cashflow FLOAT8,
    net_income FLOAT8,
    ebit FLOAT8,
    gross_profit FLOAT8,
    dividends_paid FLOAT8,
    total_liabilities FLOAT8,
    total_current_liabilities FLOAT8,
    total_assets FLOAT8,
    total_current_assets FLOAT8,
    research_development FLOAT8,
    market_cap FLOAT8,
    market_cap_change FLOAT8,
    market_cap_ranker FLOAT8,
    market_cap_ranker_change FLOAT8,
    market_cap_USD FLOAT8,
    price_earnings FLOAT8,
    price_earnings_change FLOAT8,
    price_earnings_ranker FLOAT8,
    price_earnings_ranker_change FLOAT8,
    price_earnings_growth FLOAT8,
    price_earnings_growth_change FLOAT8,
    price_earnings_growth_ranker FLOAT8,
    price_earnings_growth_ranker_change FLOAT8,
    price_sales FLOAT8,
    price_sales_change FLOAT8,
    price_sales_ranker FLOAT8,
    price_sales_ranker_change FLOAT8,
    price_sales_growth FLOAT8,
    price_sales_growth_change FLOAT8,
    price_sales_growth_ranker FLOAT8,
    price_sales_growth_ranker_change FLOAT8,
    price_book FLOAT8,
    price_book_change FLOAT8,
    price_book_ranker FLOAT8,
    price_book_ranker_change FLOAT8,
    dividend_yield FLOAT8,
    dividend_yield_change FLOAT8,
    dividend_yield_ranker FLOAT8,
    dividend_yield_ranker_change FLOAT8,
    payout_ratio FLOAT8,
    payout_ratio_change FLOAT8,
    payout_ratio_ranker FLOAT8,
    payout_ratio_ranker_change FLOAT8,
    revenue_growth_1y FLOAT8,
    revenue_growth_1y_change FLOAT8,
    revenue_growth_1y_ranker FLOAT8,
    revenue_growth_1y_ranker_change FLOAT8,
    earnings_growth_1y FLOAT8,
    earnings_growth_1y_change FLOAT8,
    earnings_growth_1y_ranker FLOAT8,
    earnings_growth_1y_ranker_change FLOAT8,
    dividend_growth_1y FLOAT8,
    dividend_growth_1y_change FLOAT8,
    dividend_growth_1y_ranker FLOAT8,
    dividend_growth_1y_ranker_change FLOAT8,
    revenue_growth_3y FLOAT8,
    revenue_growth_3y_change FLOAT8,
    revenue_growth_3y_ranker FLOAT8,
    revenue_growth_3y_ranker_change FLOAT8,
    earnings_growth_3y FLOAT8,
    earnings_growth_3y_change FLOAT8,
    earnings_growth_3y_ranker FLOAT8,
    earnings_growth_3y_ranker_change FLOAT8,
    dividend_growth_3y FLOAT8,
    dividend_growth_3y_change FLOAT8,
    dividend_growth_3y_ranker FLOAT8,
    dividend_growth_3y_ranker_change FLOAT8,
    revenue_growth_5y FLOAT8,
    revenue_growth_5y_change FLOAT8,
    revenue_growth_5y_ranker FLOAT8,
    revenue_growth_5y_ranker_change FLOAT8,
    earnings_growth_5y FLOAT8,
    earnings_growth_5y_change FLOAT8,
    earnings_growth_5y_ranker FLOAT8,
    earnings_growth_5y_ranker_change FLOAT8,
    dividend_growth_5y FLOAT8,
    dividend_growth_5y_change FLOAT8,
    dividend_growth_5y_ranker FLOAT8,
    dividend_growth_5y_ranker_change FLOAT8,
    revenue_growth_9y FLOAT8,
    revenue_growth_9y_change FLOAT8,
    revenue_growth_9y_ranker FLOAT8,
    revenue_growth_9y_ranker_change FLOAT8,
    earnings_growth_9y FLOAT8,
    earnings_growth_9y_change FLOAT8,
    earnings_growth_9y_ranker FLOAT8,
    earnings_growth_9y_ranker_change FLOAT8,
    dividend_growth_9y FLOAT8,
    dividend_growth_9y_change FLOAT8,
    dividend_growth_9y_ranker FLOAT8,
    dividend_growth_9y_ranker_change FLOAT8,
    gross_profit_margin FLOAT8,
    gross_profit_margin_change FLOAT8,
    gross_profit_margin_ranker FLOAT8,
    gross_profit_margin_ranker_change FLOAT8,
    net_profit_margin FLOAT8,
    net_profit_margin_change FLOAT8,
    net_profit_margin_ranker FLOAT8,
    net_profit_margin_ranker_change FLOAT8,
    operating_cash_flow_margin FLOAT8,
    operating_cash_flow_margin_change FLOAT8,
    operating_cash_flow_margin_ranker FLOAT8,
    operating_cash_flow_margin_ranker_change FLOAT8,
    operating_margin FLOAT8,
    operating_margin_change FLOAT8,
    operating_margin_ranker FLOAT8,
    operating_margin_ranker_change FLOAT8,
    return_on_assets FLOAT8,
    return_on_assets_change FLOAT8,
    return_on_assets_ranker FLOAT8,
    return_on_assets_ranker_change FLOAT8,
    asset_turnover FLOAT8,
    asset_turnover_change FLOAT8,
    asset_turnover_ranker FLOAT8,
    asset_turnover_ranker_change FLOAT8,
    cash_flow_on_assets FLOAT8,
    cash_flow_on_assets_change FLOAT8,
    cash_flow_on_assets_ranker FLOAT8,
    cash_flow_on_assets_ranker_change FLOAT8,
    return_on_equity FLOAT8,
    return_on_equity_change FLOAT8,
    return_on_equity_ranker FLOAT8,
    return_on_equity_ranker_change FLOAT8,
    revenue_on_equity FLOAT8,
    revenue_on_equity_change FLOAT8,
    revenue_on_equity_ranker FLOAT8,
    revenue_on_equity_ranker_change FLOAT8,
    return_on_capital_employed FLOAT8,
    return_on_capital_employed_change FLOAT8,
    return_on_capital_employed_ranker FLOAT8,
    return_on_capital_employed_ranker_change FLOAT8,
    debt_ratio FLOAT8,
    debt_ratio_change FLOAT8,
    debt_ratio_ranker FLOAT8,
    debt_ratio_ranker_change FLOAT8,
    debt_to_ebit FLOAT8,
    debt_to_ebit_change FLOAT8,
    debt_to_ebit_ranker FLOAT8,
    debt_to_ebit_ranker_change FLOAT8,
    debt_to_equity FLOAT8,
    debt_to_equity_change FLOAT8,
    debt_to_equity_ranker FLOAT8,
    debt_to_equity_ranker_change FLOAT8,
    debt_to_revenue FLOAT8,
    debt_to_revenue_change FLOAT8,
    debt_to_revenue_ranker FLOAT8,
    debt_to_revenue_ranker_change FLOAT8,
    current_ratio FLOAT8,
    current_ratio_change FLOAT8,
    current_ratio_ranker FLOAT8,
    current_ratio_ranker_change FLOAT8,
    price_cash_flow FLOAT8,
    price_cash_flow_change FLOAT8,
    price_cash_flow_ranker FLOAT8,
    price_cash_flow_ranker_change FLOAT8,
    time_arr TIMESTAMP [],
    total_revenue_arr FLOAT8 [],
    free_cashflow_arr FLOAT8 [],
    net_income_arr FLOAT8 [],
    ebit_arr FLOAT8 [],
    gross_profit_arr FLOAT8 [],
    dividends_paid_arr FLOAT8 [],
    total_liabilities_arr FLOAT8 [],
    total_current_liabilities_arr FLOAT8 [],
    total_assets_arr FLOAT8 [],
    total_current_assets_arr FLOAT8 [],
    research_development_arr FLOAT8 [],
    outstanding_shares_arr FLOAT8 [],
    price_earnings_arr FLOAT8 [],
    price_sales_arr FLOAT8 [],
    price_book_arr FLOAT8 [],
    dividend_yield_arr FLOAT8 [],
    payout_ratio_arr FLOAT8 [],
    gross_profit_margin_arr FLOAT8 [],
    net_profit_margin_arr FLOAT8 [],
    operating_cash_flow_margin_arr FLOAT8 [],
    operating_margin_arr FLOAT8 [],
    return_on_assets_arr FLOAT8 [],
    asset_turnover_arr FLOAT8 [],
    cash_flow_on_assets_arr FLOAT8 [],
    return_on_equity_arr FLOAT8 [],
    revenue_on_equity_arr FLOAT8 [],
    return_on_capital_employed_arr FLOAT8 [],
    debt_ratio_arr FLOAT8 [],
    debt_to_ebit_arr FLOAT8 [],
    debt_to_equity_arr FLOAT8 [],
    debt_to_revenue_arr FLOAT8 [],
    current_ratio_arr FLOAT8 [],
    price_cash_flow_arr FLOAT8 [],
    stock_prices FLOAT8 [] [],
    fama_french_expectation double precision,
    relative_score double precision,
    stock_price double precision,
    average_volume double precision,
    fama_french_expectation_ranker double precision,
    piotroski_score double precision,
    SMB_factor double precision,
    HML_factor double precision,
    CMA_factor double precision,
    RMW_factor double precision,
    excess_market_return_factor double precision,
    cluster int,
    relative_score_continuous double precision,
    rsi_180 double precision,
    combined_score double precision,
    implied_volatility double precision,
    implied_volatility_ranker double precision,
    intrinsic_risk double precision,
    equity_risk double precision,
    interest_rate_risk double precision,
    credit_risk double precision,
    commodities_risk double precision,
    inflation_risk double precision,
    esg boolean,
    daily_return double precision,
    weekly_return double precision,
    monthly_return double precision,
    quarterly_return double precision,
    semi_annual_return double precision,
    annual_return double precision,
    two_year_return double precision,
    three_year_return double precision,
    price_earnings_average_10y double precision,
    price_earnings_deviation_10y double precision,
    price_book_average_10y double precision,
    price_book_deviation_10y double precision,
    price_sales_average_10y double precision,
    price_sales_deviation_10y double precision,
    price_cash_flow_average_10y double precision,
    price_cash_flow_deviation_10y double precision,
    volume_deviation double precision,
    PRIMARY KEY (ticker)
)