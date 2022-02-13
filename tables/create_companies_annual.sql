CREATE TABLE companies_annual (
    ticker character varying(32),
    currency character varying(32),
    forex_rate double precision,
    time timestamp without time zone,
    industry character varying(255),
    sector character varying(255),
    total_assets double precision,
    intangible_assets double precision,
    earning_assets double precision,
    other_current_assets double precision,
    other_assets double precision,
    total_current_assets double precision,
    net_tangible_assets double precision,
    deferred_long_term_asset_charges double precision,
    non_current_assets_other double precision,
    non_current_assets_total double precision,
    total_liabilities double precision,
    deferred_longterm_liabilities double precision,
    other_current_liabilities double precision,
    other_liabilities double precision,
    total_current_liabilities double precision,
    cash_and_short_term_investments double precision,
    cash double precision,
    property_plant_and_equipment_gross double precision,
    net_working_capital double precision,
    net_invested_capital double precision,
    net_debt double precision,
    short_term_debt double precision,
    short_long_term_debt double precision,
    short_long_term_debt_total double precision,
    long_term_debt double precision,
    capital_lease_obligations double precision,
    accumulated_amortization double precision,
    accumulated_depreciation double precision,
    long_term_debt_total double precision,
    non_current_liabilities_other double precision,
    non_current_liabilities_total double precision,
    total_permanent_equity double precision,
    other_stockholder_equity double precision,
    total_stockholder_equity double precision,
    common_stock_total_equity double precision,
    preferred_stock_total_equity double precision,
    retained_earnings_total_equity double precision,
    liabilities_and_stockholders_equity double precision,
    common_stock double precision,
    retained_earnings double precision,
    goodwill double precision,
    property_plant_equipment double precision,
    long_term_investments double precision,
    short_term_investments double precision,
    net_receivables double precision,
    inventory double precision,
    accounts_payable double precision,
    noncontrolling_interest_in_consolidated_entity double precision,
    temporary_equity_redeemable_noncontrolling_interests double precision,
    accumulated_other_comprehensive_income double precision,
    additional_paid_in_capital double precision,
    treasury_stock double precision,
    negative_goodwill double precision,
    warrants double precision,
    preferred_stock_redeemable double precision,
    capital_surplus double precision,
    common_stock_shares_outstanding double precision,
    investments double precision,
    change_to_liabilities double precision,
    total_cashflows_from_investing_activities double precision,
    net_borrowings double precision,
    total_cash_from_financing_activities double precision,
    change_to_operating_activities double precision,
    change_in_cash double precision,
    begin_period_cashflow double precision,
    end_period_cashflow double precision,
    total_cash_from_operating_activities double precision,
    depreciation double precision,
    other_cashflows_from_investing_activities double precision,
    dividends_paid double precision,
    change_to_inventory double precision,
    change_to_account_receivables double precision,
    sale_purchase_of_stock double precision,
    other_cashflows_from_financing_activities double precision,
    change_to_net_income double precision,
    capital_expenditures double precision,
    change_receivables double precision,
    cashflows_other_operating double precision,
    exchange_rate_changes double precision,
    cash_and_cash_equivalents_changes double precision,
    change_in_working_capital double precision,
    other_non_cash_items double precision,
    free_cashflow double precision,
    research_development double precision,
    effect_of_accounting_charges double precision,
    income_before_tax double precision,
    minority_interest double precision,
    net_income double precision,
    selling_general_administrative double precision,
    selling_and_marketing_expenses double precision,
    gross_profit double precision,
    reconciled_depreciation double precision,
    ebit double precision,
    ebitda double precision,
    depreciation_and_amortization double precision,
    non_operating_income_net_other double precision,
    operating_income double precision,
    other_operating_expenses double precision,
    interest_expense double precision,
    tax_provision double precision,
    interest_income double precision,
    net_interest_income double precision,
    extraordinary_items double precision,
    non_recurring double precision,
    other_items double precision,
    income_tax_expense double precision,
    total_revenue double precision,
    total_operating_expenses double precision,
    cost_of_revenue double precision,
    total_other_income_expense_net double precision,
    discontinued_operations double precision,
    net_income_from_continuing_operations double precision,
    net_income_applicable_to_common_shares double precision,
    preferred_stock_and_other_adjustments double precision,
    outstanding_shares double precision,
    market_cap double precision,
    market_cap_change double precision,
    market_cap_ranker double precision,
    market_cap_ranker_change double precision,
    price_earnings double precision,
    price_earnings_change double precision,
    price_earnings_ranker double precision,
    price_earnings_ranker_change double precision,
    price_earnings_growth double precision,
    price_earnings_growth_change double precision,
    price_earnings_growth_ranker double precision,
    price_earnings_growth_ranker_change double precision,
    price_sales double precision,
    price_sales_change double precision,
    price_sales_ranker double precision,
    price_sales_ranker_change double precision,
    price_sales_growth double precision,
    price_sales_growth_change double precision,
    price_sales_growth_ranker double precision,
    price_sales_growth_ranker_change double precision,
    price_book double precision,
    price_book_change double precision,
    price_book_ranker double precision,
    price_book_ranker_change double precision,
    dividend_yield double precision,
    dividend_yield_change double precision,
    dividend_yield_ranker double precision,
    dividend_yield_ranker_change double precision,
    payout_ratio double precision,
    payout_ratio_change double precision,
    payout_ratio_ranker double precision,
    payout_ratio_ranker_change double precision,
    revenue_growth_1y double precision,
    revenue_growth_1y_change double precision,
    revenue_growth_1y_ranker double precision,
    revenue_growth_1y_ranker_change double precision,
    earnings_growth_1y double precision,
    earnings_growth_1y_change double precision,
    earnings_growth_1y_ranker double precision,
    earnings_growth_1y_ranker_change double precision,
    dividend_growth_1y double precision,
    dividend_growth_1y_change double precision,
    dividend_growth_1y_ranker double precision,
    dividend_growth_1y_ranker_change double precision,
    revenue_growth_3y double precision,
    revenue_growth_3y_change double precision,
    revenue_growth_3y_ranker double precision,
    revenue_growth_3y_ranker_change double precision,
    earnings_growth_3y double precision,
    earnings_growth_3y_change double precision,
    earnings_growth_3y_ranker double precision,
    earnings_growth_3y_ranker_change double precision,
    dividend_growth_3y double precision,
    dividend_growth_3y_change double precision,
    dividend_growth_3y_ranker double precision,
    dividend_growth_3y_ranker_change double precision,
    revenue_growth_5y double precision,
    revenue_growth_5y_change double precision,
    revenue_growth_5y_ranker double precision,
    revenue_growth_5y_ranker_change double precision,
    earnings_growth_5y double precision,
    earnings_growth_5y_change double precision,
    earnings_growth_5y_ranker double precision,
    earnings_growth_5y_ranker_change double precision,
    dividend_growth_5y double precision,
    dividend_growth_5y_change double precision,
    dividend_growth_5y_ranker double precision,
    dividend_growth_5y_ranker_change double precision,
    revenue_growth_9y double precision,
    revenue_growth_9y_change double precision,
    revenue_growth_9y_ranker double precision,
    revenue_growth_9y_ranker_change double precision,
    earnings_growth_9y double precision,
    earnings_growth_9y_change double precision,
    earnings_growth_9y_ranker double precision,
    earnings_growth_9y_ranker_change double precision,
    dividend_growth_9y double precision,
    dividend_growth_9y_change double precision,
    dividend_growth_9y_ranker double precision,
    dividend_growth_9y_ranker_change double precision,
    gross_profit_margin double precision,
    gross_profit_margin_change double precision,
    gross_profit_margin_ranker double precision,
    gross_profit_margin_ranker_change double precision,
    net_profit_margin double precision,
    net_profit_margin_change double precision,
    net_profit_margin_ranker double precision,
    net_profit_margin_ranker_change double precision,
    operating_cash_flow_margin double precision,
    operating_cash_flow_margin_change double precision,
    operating_cash_flow_margin_ranker double precision,
    operating_cash_flow_margin_ranker_change double precision,
    operating_margin double precision,
    operating_margin_change double precision,
    operating_margin_ranker double precision,
    operating_margin_ranker_change double precision,
    return_on_assets double precision,
    return_on_assets_change double precision,
    return_on_assets_ranker double precision,
    return_on_assets_ranker_change double precision,
    asset_turnover double precision,
    asset_turnover_change double precision,
    asset_turnover_ranker double precision,
    asset_turnover_ranker_change double precision,
    cash_flow_on_assets double precision,
    cash_flow_on_assets_change double precision,
    cash_flow_on_assets_ranker double precision,
    cash_flow_on_assets_ranker_change double precision,
    return_on_equity double precision,
    return_on_equity_change double precision,
    return_on_equity_ranker double precision,
    return_on_equity_ranker_change double precision,
    revenue_on_equity double precision,
    revenue_on_equity_change double precision,
    revenue_on_equity_ranker double precision,
    revenue_on_equity_ranker_change double precision,
    return_on_capital_employed double precision,
    return_on_capital_employed_change double precision,
    return_on_capital_employed_ranker double precision,
    return_on_capital_employed_ranker_change double precision,
    debt_ratio double precision,
    debt_ratio_change double precision,
    debt_ratio_ranker double precision,
    debt_ratio_ranker_change double precision,
    debt_to_ebit double precision,
    debt_to_ebit_change double precision,
    debt_to_ebit_ranker double precision,
    debt_to_ebit_ranker_change double precision,
    debt_to_equity double precision,
    debt_to_equity_change double precision,
    debt_to_equity_ranker double precision,
    debt_to_equity_ranker_change double precision,
    debt_to_revenue double precision,
    debt_to_revenue_change double precision,
    debt_to_revenue_ranker double precision,
    debt_to_revenue_ranker_change double precision,
    current_ratio double precision,
    current_ratio_change double precision,
    current_ratio_ranker double precision,
    current_ratio_ranker_change double precision,
    price_cash_flow double precision,
    price_cash_flow_change double precision,
    price_cash_flow_ranker double precision,
    price_cash_flow_ranker_change double precision,
    relative_score double precision,
    contained_in_s_and_p500 boolean,
    piotroski_score double precision,
    outstanding_shares_change double precision,
    relative_score_continuous double precision,
    return_on_capital double precision,
    return_on_capital_change double precision,
    return_on_capital_ranker double precision,
    return_on_capital_ranker_change double precision,
    return_on_net_tangible_assets double precision,
    return_on_net_tangible_assets_change double precision,
    return_on_net_tangible_assets_ranker double precision,
    return_on_net_tangible_assets_ranker_change double precision,
    price_ebit double precision,
    price_ebit_ranker double precision,
    price_ebit_change double precision,
    price_ebit_ranker_change double precision,
    ev double precision,
    ev_ebit double precision,
    ev_ebitda double precision,
    ev_ebit_ranker double precision,
    ev_ebitda_ranker double precision,
    ev_ebit_change double precision,
    ev_ebitda_change double precision,
    ev_ebit_ranker_change double precision,
    ev_ebitda_ranker_change double precision,
    PRIMARY KEY (ticker, time)
)