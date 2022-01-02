from time import time


def calculate_ttm(db):
    '''
    Calculates ttm fundamentals.
    '''

    print('Calculating ttm fundamentals...')

    start = time()

    sql = '''
        WITH ttm_table AS (
            SELECT 
                ticker,
                time,
                SUM(net_income) OVER w net_income_ttm,
                SUM(total_revenue) OVER w total_revenue_ttm,
                SUM(free_cashflow) OVER w free_cashflow_ttm,
                SUM(dividends_paid) OVER w dividends_paid_ttm,
                SUM(gross_profit) OVER w gross_profit_ttm,
                SUM(total_cash_from_operating_activities) OVER w total_cash_from_operating_activities_ttm,
                SUM(ebit) OVER w ebit_ttm
            FROM companies_quarterly
            WINDOW w AS (
                PARTITION BY ticker
                ORDER BY time
                RANGE BETWEEN INTERVAL '365 days' PRECEDING AND CURRENT ROW
            )
        )
        UPDATE companies_quarterly c
        SET
            net_income_ttm = ttm_table.net_income_ttm,
            total_revenue_ttm = ttm_table.total_revenue_ttm,
            free_cashflow_ttm = ttm_table.free_cashflow_ttm,
            dividends_paid_ttm = ttm_table.dividends_paid_ttm,
            gross_profit_ttm = ttm_table.gross_profit_ttm,
            total_cash_from_operating_activities_ttm = ttm_table.total_cash_from_operating_activities_ttm,
            ebit_ttm = ttm_table.ebit_ttm
        FROM ttm_table
        WHERE 
            c.ticker = ttm_table.ticker
        AND
            c.time = ttm_table.time
        ;
    '''

    db.execute(sql)
    db.commit()

    end = time()

    print(f'Finished calculating ttm fundamentals. ({end - start:.2f}s)')


def calculate_metrics(db):
    '''
    Calculates ttm financial metrics and updates db.
    '''

    print('Calculating ttm metrics...')

    start = time()

    sql = '''
        UPDATE companies_quarterly
        SET
            price_earnings = (CASE WHEN net_income > 0 THEN market_cap ELSE NULL END) / NULLIF(net_income, 0),
            price_sales = market_cap / NULLIF(total_revenue, 0),
            price_cash_flow = (CASE WHEN free_cashflow > 0 THEN market_cap ELSE NULL END) / NULLIF(free_cashflow, 0),
            price_book = (CASE WHEN total_assets - total_liabilities > 0 THEN market_cap ELSE NULL END) / NULLIF(total_assets - total_liabilities, 0),
            dividend_yield = 100 * ABS(dividends_paid) / NULLIF(market_cap, 0),
            payout_ratio = 100 * ABS(dividends_paid) / NULLIF(net_income, 0),
            gross_profit_margin = 100 * gross_profit_ttm / NULLIF(total_revenue_ttm, 0),
            net_profit_margin = 100 * net_income_ttm / NULLIF(total_revenue_ttm, 0),
            operating_cash_flow_margin = 100 * total_cash_from_operating_activities_ttm / NULLIF(total_revenue_ttm, 0),
            operating_margin = 100 * ebit_ttm / NULLIF(total_revenue_ttm, 0),
            return_on_assets = 100 * net_income_ttm / NULLIF(total_assets, 0),
            asset_turnover = 100 * total_revenue_ttm / NULLIF(total_assets, 0),
            cash_flow_on_assets = 100 * free_cashflow_ttm / NULLIF(total_assets, 0),
            return_on_equity = 100 * (CASE WHEN (total_assets - total_liabilities) > 0 THEN net_income_ttm ELSE NULL END) / NULLIF(total_assets - total_liabilities, 0),
            revenue_on_equity = 100 * (CASE WHEN (total_assets - total_liabilities) > 0 THEN total_revenue_ttm ELSE NULL END) / NULLIF(total_assets - total_liabilities, 0),
            return_on_capital_employed = 100 * net_income_ttm / NULLIF(total_assets - total_current_liabilities, 0),
            debt_ratio = 100 * total_liabilities / NULLIF(total_assets, 0),
            debt_to_ebit = (CASE WHEN ebit_ttm > 0 THEN total_liabilities ELSE NULL END) / NULLIF(ebit_ttm, 0),
            debt_to_equity = (CASE WHEN (total_assets - total_liabilities) > 0 THEN total_liabilities ELSE NULL END) / NULLIF(total_assets - total_liabilities, 0),
            debt_to_revenue = total_liabilities / NULLIF(total_revenue_ttm, 0),
            current_ratio = total_current_assets / NULLIF(total_current_liabilities, 0)
        ;
    '''

    db.execute(sql)
    db.commit()

    end = time()

    print(f'Finished calculating ttm metrics. ({end - start:.2f}s)')


def calculate_growth(db):
    '''
    Calculates growth metrics and updates db.
    '''

    print('Calculating growth metrics...')

    start = time()

    sql = '''
        WITH cte AS (	
            SELECT
                ticker,
                time,
                LAG(total_revenue_ttm, 4) OVER w revenue_1,
                LAG(total_revenue_ttm, 12) OVER w revenue_3,
                LAG(total_revenue_ttm, 20) OVER w revenue_5,
                LAG(total_revenue_ttm, 36) OVER w revenue_9,
                LAG(net_income_ttm, 4) OVER w net_income_1,
                LAG(net_income_ttm, 12) OVER w net_income_3,
                LAG(net_income_ttm, 20) OVER w net_income_5,
                LAG(net_income_ttm, 36) OVER w net_income_9,
                LAG(dividends_paid_ttm, 4) OVER w dividend_1,
                LAG(dividends_paid_ttm, 12) OVER w dividend_3,
                LAG(dividends_paid_ttm, 20) OVER w dividend_5,
                LAG(dividends_paid_ttm, 36) OVER w dividend_9
            FROM companies_quarterly
            WINDOW w AS (
                PARTITION BY ticker
                ORDER BY time
            )
        ) 
        UPDATE companies_quarterly c
        SET
            price_earnings_growth = c.price_earnings / NULLIF(100 * (c.net_income_ttm / NULLIF(cte.net_income_1, 0) - 1), 0),
            price_sales_growth = c.price_sales / NULLIF(100 * (c.total_revenue_ttm / NULLIF(cte.revenue_1, 0) - 1), 0),
            revenue_growth_1y = 100 * ((CASE WHEN c.total_revenue_ttm > 0 THEN c.total_revenue_ttm ELSE NULL END) / (CASE WHEN cte.revenue_1 > 0 THEN cte.revenue_1 ELSE NULL END) - 1),
            revenue_growth_3y = 100 * (((CASE WHEN c.total_revenue_ttm > 0 THEN c.total_revenue_ttm ELSE NULL END) / (CASE WHEN cte.revenue_3 > 0 THEN cte.revenue_3 ELSE NULL END)) ^ (1.0/3) - 1),
            revenue_growth_5y = 100 * (((CASE WHEN c.total_revenue_ttm > 0 THEN c.total_revenue_ttm ELSE NULL END) / (CASE WHEN cte.revenue_5 > 0 THEN cte.revenue_5 ELSE NULL END)) ^ (1.0/5) - 1),
            revenue_growth_9y = 100 * (((CASE WHEN c.total_revenue_ttm > 0 THEN c.total_revenue_ttm ELSE NULL END) / (CASE WHEN cte.revenue_9 > 0 THEN cte.revenue_9 ELSE NULL END)) ^ (1.0/9) - 1),
            earnings_growth_1y = 100 * ((CASE WHEN c.net_income_ttm > 0 THEN c.net_income_ttm ELSE NULL END) / (CASE WHEN cte.net_income_1 > 0 THEN cte.net_income_1 ELSE NULL END) - 1),
            earnings_growth_3y = 100 * (((CASE WHEN c.net_income_ttm > 0 THEN c.net_income_ttm ELSE NULL END) / (CASE WHEN cte.net_income_3 > 0 THEN cte.net_income_3 ELSE NULL END)) ^ (1.0/3) - 1),
            earnings_growth_5y = 100 * (((CASE WHEN c.net_income_ttm > 0 THEN c.net_income_ttm ELSE NULL END) / (CASE WHEN cte.net_income_5 > 0 THEN cte.net_income_5 ELSE NULL END)) ^ (1.0/5) - 1),
            earnings_growth_9y = 100 * (((CASE WHEN c.net_income_ttm > 0 THEN c.net_income_ttm ELSE NULL END) / (CASE WHEN cte.net_income_9 > 0 THEN cte.net_income_9 ELSE NULL END)) ^ (1.0/9) - 1),
            dividend_growth_1y = 100 * (ABS(c.dividends_paid_ttm) / NULLIF(ABS(cte.dividend_1), 0) - 1),
            dividend_growth_3y = 100 * ((ABS(c.dividends_paid_ttm) / NULLIF(ABS(cte.dividend_3), 0)) ^ (1.0/3) - 1),
            dividend_growth_5y = 100 * ((ABS(c.dividends_paid_ttm) / NULLIF(ABS(cte.dividend_5), 0)) ^ (1.0/5) - 1),
            dividend_growth_9y = 100 * ((ABS(c.dividends_paid_ttm) / NULLIF(ABS(cte.dividend_9), 0)) ^ (1.0/9) - 1)
        FROM cte
        WHERE c.ticker = cte.ticker
        AND c.time = cte.time
        ;
    '''

    db.execute(sql)
    db.commit()

    end = time()

    print(f'Finished calculating growth metrics. ({end - start:.2f}s)')


def calculate_rankings(db):
    '''
    Calculates the relative ranking of metrics in the respective sector.
    '''

    print('Calculating rankings...')

    start = time()

    sql = '''
        WITH cte AS (
            SELECT 
                ticker, 
                time,
                sector,
                PERCENT_RANK() OVER(
                    PARTITION BY (sector, EXTRACT(QUARTER FROM time), EXTRACT(YEAR FROM time), market_cap IS NOT NULL) 
                    ORDER BY market_cap
                ) market_cap_rel,
                PERCENT_RANK() OVER(
                    PARTITION BY (sector, EXTRACT(QUARTER FROM time), EXTRACT(YEAR FROM time), price_earnings IS NOT NULL)
                    ORDER BY price_earnings DESC
                ) price_earnings_rel,
                PERCENT_RANK() OVER(
                    PARTITION BY (sector, EXTRACT(QUARTER FROM time), EXTRACT(YEAR FROM time), price_earnings_growth IS NOT NULL)
                    ORDER BY price_earnings_growth DESC
                ) price_earnings_growth_rel,
                PERCENT_RANK() OVER(
                    PARTITION BY (sector, EXTRACT(QUARTER FROM time), EXTRACT(YEAR FROM time), price_sales IS NOT NULL)
                    ORDER BY price_sales DESC
                ) price_sales_rel,
                PERCENT_RANK() OVER(
                    PARTITION BY (sector, EXTRACT(QUARTER FROM time), EXTRACT(YEAR FROM time), price_sales_growth IS NOT NULL)
                    ORDER BY price_sales_growth DESC
                ) price_sales_growth_rel,
                PERCENT_RANK() OVER(
                    PARTITION BY (sector, EXTRACT(QUARTER FROM time), EXTRACT(YEAR FROM time), price_cash_flow IS NOT NULL)
                    ORDER BY price_cash_flow DESC
                ) price_cash_flow_rel,
                PERCENT_RANK() OVER(
                    PARTITION BY (sector, EXTRACT(QUARTER FROM time), EXTRACT(YEAR FROM time), price_book IS NOT NULL)
                    ORDER BY price_book DESC
                ) price_book_rel,
                PERCENT_RANK() OVER(
                    PARTITION BY (sector, EXTRACT(QUARTER FROM time), EXTRACT(YEAR FROM time), dividend_yield IS NOT NULL)
                    ORDER BY dividend_yield
                ) dividend_yield_rel,
                PERCENT_RANK() OVER(
                    PARTITION BY (sector, EXTRACT(QUARTER FROM time), EXTRACT(YEAR FROM time), payout_ratio IS NOT NULL)
                    ORDER BY payout_ratio
                ) payout_ratio_rel,
                PERCENT_RANK() OVER(
                    PARTITION BY (sector, EXTRACT(QUARTER FROM time), EXTRACT(YEAR FROM time), revenue_growth_1y IS NOT NULL)
                    ORDER BY revenue_growth_1y
                ) revenue_growth_1y_rel,
                PERCENT_RANK() OVER(
                    PARTITION BY (sector, EXTRACT(QUARTER FROM time), EXTRACT(YEAR FROM time), earnings_growth_1y IS NOT NULL)
                    ORDER BY earnings_growth_1y
                ) earnings_growth_1y_rel,
                PERCENT_RANK() OVER(
                    PARTITION BY (sector, EXTRACT(QUARTER FROM time), EXTRACT(YEAR FROM time), dividend_growth_1y IS NOT NULL)
                    ORDER BY dividend_growth_1y
                ) dividend_growth_1y_rel,
                PERCENT_RANK() OVER(
                    PARTITION BY (sector, EXTRACT(QUARTER FROM time), EXTRACT(YEAR FROM time), revenue_growth_3y IS NOT NULL)
                    ORDER BY revenue_growth_3y
                ) revenue_growth_3y_rel,
                PERCENT_RANK() OVER(
                    PARTITION BY (sector, EXTRACT(QUARTER FROM time), EXTRACT(YEAR FROM time), earnings_growth_3y IS NOT NULL)
                    ORDER BY earnings_growth_3y
                ) earnings_growth_3y_rel,
                PERCENT_RANK() OVER(
                    PARTITION BY (sector, EXTRACT(QUARTER FROM time), EXTRACT(YEAR FROM time), dividend_growth_3y IS NOT NULL)
                    ORDER BY dividend_growth_3y
                ) dividend_growth_3y_rel,
                PERCENT_RANK() OVER(
                    PARTITION BY (sector, EXTRACT(QUARTER FROM time), EXTRACT(YEAR FROM time), revenue_growth_5y IS NOT NULL)
                    ORDER BY revenue_growth_5y
                ) revenue_growth_5y_rel,
                PERCENT_RANK() OVER(
                    PARTITION BY (sector, EXTRACT(QUARTER FROM time), EXTRACT(YEAR FROM time), earnings_growth_5y IS NOT NULL)
                    ORDER BY earnings_growth_5y
                ) earnings_growth_5y_rel,
                PERCENT_RANK() OVER(
                    PARTITION BY (sector, EXTRACT(QUARTER FROM time), EXTRACT(YEAR FROM time), dividend_growth_5y IS NOT NULL)
                    ORDER BY dividend_growth_5y
                ) dividend_growth_5y_rel,
                PERCENT_RANK() OVER(
                    PARTITION BY (sector, EXTRACT(QUARTER FROM time), EXTRACT(YEAR FROM time), revenue_growth_9y IS NOT NULL)
                    ORDER BY revenue_growth_9y
                ) revenue_growth_9y_rel,
                PERCENT_RANK() OVER(
                    PARTITION BY (sector, EXTRACT(QUARTER FROM time), EXTRACT(YEAR FROM time), earnings_growth_9y IS NOT NULL)
                    ORDER BY earnings_growth_9y
                ) earnings_growth_9y_rel,
                PERCENT_RANK() OVER(
                    PARTITION BY (sector, EXTRACT(QUARTER FROM time), EXTRACT(YEAR FROM time), dividend_growth_9y IS NOT NULL)
                    ORDER BY dividend_growth_9y
                ) dividend_growth_9y_rel,
                PERCENT_RANK() OVER(
                    PARTITION BY (sector, EXTRACT(QUARTER FROM time), EXTRACT(YEAR FROM time), gross_profit_margin IS NOT NULL)
                    ORDER BY gross_profit_margin
                ) gross_profit_margin_rel,
                PERCENT_RANK() OVER(
                    PARTITION BY (sector, EXTRACT(QUARTER FROM time), EXTRACT(YEAR FROM time), net_profit_margin IS NOT NULL)
                    ORDER BY net_profit_margin
                ) net_profit_margin_rel,
                PERCENT_RANK() OVER(
                    PARTITION BY (sector, EXTRACT(QUARTER FROM time), EXTRACT(YEAR FROM time), operating_cash_flow_margin IS NOT NULL)
                    ORDER BY operating_cash_flow_margin
                ) operating_cash_flow_margin_rel,
                PERCENT_RANK() OVER(
                    PARTITION BY (sector, EXTRACT(QUARTER FROM time), EXTRACT(YEAR FROM time), operating_margin IS NOT NULL)
                    ORDER BY operating_margin
                ) operating_margin_rel,
                PERCENT_RANK() OVER(
                    PARTITION BY (sector, EXTRACT(QUARTER FROM time), EXTRACT(YEAR FROM time), return_on_assets IS NOT NULL)
                    ORDER BY return_on_assets
                ) return_on_assets_rel,
                PERCENT_RANK() OVER(
                    PARTITION BY (sector, EXTRACT(QUARTER FROM time), EXTRACT(YEAR FROM time), asset_turnover IS NOT NULL)
                    ORDER BY asset_turnover
                ) asset_turnover_rel,
                PERCENT_RANK() OVER(
                    PARTITION BY (sector, EXTRACT(QUARTER FROM time), EXTRACT(YEAR FROM time), cash_flow_on_assets IS NOT NULL)
                    ORDER BY cash_flow_on_assets
                ) cash_flow_on_assets_rel,
                PERCENT_RANK() OVER(
                    PARTITION BY (sector, EXTRACT(QUARTER FROM time), EXTRACT(YEAR FROM time), return_on_equity IS NOT NULL)
                    ORDER BY return_on_equity
                ) return_on_equity_rel,
                PERCENT_RANK() OVER(
                    PARTITION BY (sector, EXTRACT(QUARTER FROM time), EXTRACT(YEAR FROM time), revenue_on_equity IS NOT NULL)
                    ORDER BY revenue_on_equity
                ) revenue_on_equity_rel,
                PERCENT_RANK() OVER(
                    PARTITION BY (sector, EXTRACT(QUARTER FROM time), EXTRACT(YEAR FROM time), return_on_capital_employed IS NOT NULL)
                    ORDER BY return_on_capital_employed
                ) return_on_capital_employed_rel,
                PERCENT_RANK() OVER(
                    PARTITION BY (sector, EXTRACT(QUARTER FROM time), EXTRACT(YEAR FROM time), debt_ratio IS NOT NULL)
                    ORDER BY debt_ratio DESC
                ) debt_ratio_rel,
                PERCENT_RANK() OVER(
                    PARTITION BY (sector, EXTRACT(QUARTER FROM time), EXTRACT(YEAR FROM time), debt_to_ebit IS NOT NULL)
                    ORDER BY debt_to_ebit DESC
                ) debt_to_ebit_rel,
                PERCENT_RANK() OVER(
                    PARTITION BY (sector, EXTRACT(QUARTER FROM time), EXTRACT(YEAR FROM time), debt_to_equity IS NOT NULL)
                    ORDER BY debt_to_equity DESC
                ) debt_to_equity_rel,
                PERCENT_RANK() OVER(
                    PARTITION BY (sector, EXTRACT(QUARTER FROM time), EXTRACT(YEAR FROM time), debt_to_revenue IS NOT NULL)
                    ORDER BY debt_to_revenue DESC
                ) debt_to_revenue_rel,
                PERCENT_RANK() OVER(
                    PARTITION BY (sector, EXTRACT(QUARTER FROM time), EXTRACT(YEAR FROM time), current_ratio IS NOT NULL)
                    ORDER BY current_ratio
                ) current_ratio_rel
            FROM companies_quarterly
        )
        UPDATE companies_quarterly c
        SET
            market_cap_ranker = CASE WHEN c.market_cap IS NULL THEN NULL ELSE cte.market_cap_rel END,
            price_earnings_ranker = CASE WHEN c.price_earnings IS NULL THEN NULL ELSE cte.price_earnings_rel END,
            price_earnings_growth_ranker = CASE WHEN c.price_earnings_growth IS NULL THEN NULL ELSE cte.price_earnings_growth_rel END,
            price_sales_ranker = CASE WHEN c.price_sales IS NULL THEN NULL ELSE cte.price_sales_rel END,
            price_sales_growth_ranker = CASE WHEN c.price_sales_growth IS NULL THEN NULL ELSE cte.price_sales_growth_rel END,
            price_cash_flow_ranker = CASE WHEN c.price_cash_flow IS NULL THEN NULL ELSE cte.price_cash_flow_rel END,
            price_book_ranker = CASE WHEN c.price_book IS NULL THEN NULL ELSE cte.price_book_rel END,
            dividend_yield_ranker = CASE WHEN c.dividend_yield IS NULL THEN NULL ELSE cte.dividend_yield_rel END,
            payout_ratio_ranker = CASE WHEN c.payout_ratio IS NULL THEN NULL ELSE cte.payout_ratio_rel END,
            revenue_growth_1y_ranker = CASE WHEN c.revenue_growth_1y IS NULL THEN NULL ELSE cte.revenue_growth_1y_rel END,
            earnings_growth_1y_ranker = CASE WHEN c.earnings_growth_1y IS NULL THEN NULL ELSE cte.earnings_growth_1y_rel END,
            dividend_growth_1y_ranker = CASE WHEN c.dividend_growth_1y IS NULL THEN NULL ELSE cte.dividend_growth_1y_rel END,
            revenue_growth_3y_ranker = CASE WHEN c.revenue_growth_3y IS NULL THEN NULL ELSE cte.revenue_growth_3y_rel END,
            earnings_growth_3y_ranker = CASE WHEN c.earnings_growth_3y IS NULL THEN NULL ELSE cte.earnings_growth_3y_rel END,
            dividend_growth_3y_ranker = CASE WHEN c.dividend_growth_3y IS NULL THEN NULL ELSE cte.dividend_growth_3y_rel END,
            revenue_growth_5y_ranker = CASE WHEN c.revenue_growth_5y IS NULL THEN NULL ELSE cte.revenue_growth_5y_rel END,
            earnings_growth_5y_ranker = CASE WHEN c.earnings_growth_5y IS NULL THEN NULL ELSE cte.earnings_growth_5y_rel END,
            dividend_growth_5y_ranker = CASE WHEN c.dividend_growth_5y IS NULL THEN NULL ELSE cte.dividend_growth_5y_rel END,
            revenue_growth_9y_ranker = CASE WHEN c.revenue_growth_9y IS NULL THEN NULL ELSE cte.revenue_growth_9y_rel END,
            earnings_growth_9y_ranker = CASE WHEN c.earnings_growth_9y IS NULL THEN NULL ELSE cte.earnings_growth_9y_rel END,
            dividend_growth_9y_ranker = CASE WHEN c.dividend_growth_9y IS NULL THEN NULL ELSE cte.dividend_growth_9y_rel END,
            gross_profit_margin_ranker = CASE WHEN c.gross_profit_margin IS NULL THEN NULL ELSE cte.gross_profit_margin_rel END,
            net_profit_margin_ranker = CASE WHEN c.net_profit_margin IS NULL THEN NULL ELSE cte.net_profit_margin_rel END,
            operating_cash_flow_margin_ranker = CASE WHEN c.operating_cash_flow_margin IS NULL THEN NULL ELSE cte.operating_cash_flow_margin_rel END,
            operating_margin_ranker = CASE WHEN c.operating_margin IS NULL THEN NULL ELSE cte.operating_margin_rel END,
            return_on_assets_ranker = CASE WHEN c.return_on_assets IS NULL THEN NULL ELSE cte.return_on_assets_rel END,
            asset_turnover_ranker = CASE WHEN c.asset_turnover IS NULL THEN NULL ELSE cte.asset_turnover_rel END,
            cash_flow_on_assets_ranker = CASE WHEN c.cash_flow_on_assets IS NULL THEN NULL ELSE cte.cash_flow_on_assets_rel END,
            return_on_equity_ranker = CASE WHEN c.return_on_equity IS NULL THEN NULL ELSE cte.return_on_equity_rel END,
            revenue_on_equity_ranker = CASE WHEN c.revenue_on_equity IS NULL THEN NULL ELSE cte.revenue_on_equity_rel END,
            return_on_capital_employed_ranker = CASE WHEN c.return_on_capital_employed IS NULL THEN NULL ELSE cte.return_on_capital_employed_rel END,
            debt_ratio_ranker = CASE WHEN c.debt_ratio IS NULL THEN NULL ELSE cte.debt_ratio_rel END,
            debt_to_ebit_ranker = CASE WHEN c.debt_to_ebit IS NULL THEN NULL ELSE cte.debt_to_ebit_rel END,
            debt_to_equity_ranker = CASE WHEN c.debt_to_equity IS NULL THEN NULL ELSE cte.debt_to_equity_rel END,
            debt_to_revenue_ranker = CASE WHEN c.debt_to_revenue IS NULL THEN NULL ELSE cte.debt_to_revenue_rel END,
            current_ratio_ranker = CASE WHEN c.current_ratio IS NULL THEN NULL ELSE cte.current_ratio_rel END
        FROM cte
        WHERE c.ticker = cte.ticker
        AND c.time = cte.time
        ;
    '''

    db.execute(sql)
    db.commit()

    end = time()

    print(f'Finished calculating rankings. ({end - start:.2f}s)')


def calculate_change(db):
    '''
    Calculates ttm change of metrics and updates db.
    '''

    print('Calculating ttm changes...')

    start = time()

    sql = '''
        WITH cte AS (	
            SELECT
                ticker,
                time,
                LAG(outstanding_shares, 4) OVER w outstanding_shares_1,
                LAG(market_cap, 4) OVER w market_cap_1,
                LAG(market_cap_ranker, 4) OVER w market_cap_ranker_1,
                LAG(price_earnings, 4) OVER w price_earnings_1,
                LAG(price_earnings_ranker, 4) OVER w price_earnings_ranker_1,
                LAG(price_earnings_growth, 4) OVER w price_earnings_growth_1,
                LAG(price_earnings_growth_ranker, 4) OVER w price_earnings_growth_ranker_1,
                LAG(price_sales, 4) OVER w price_sales_1,
                LAG(price_sales_ranker, 4) OVER w price_sales_ranker_1,
                LAG(price_sales_growth, 4) OVER w price_sales_growth_1,
                LAG(price_sales_growth_ranker, 4) OVER w price_sales_growth_ranker_1,
                LAG(price_cash_flow, 4) OVER w price_cash_flow_1,
                LAG(price_cash_flow_ranker, 4) OVER w price_cash_flow_ranker_1,
                LAG(price_book, 4) OVER w price_book_1,
                LAG(price_book_ranker, 4) OVER w price_book_ranker_1,
                LAG(dividend_yield, 4) OVER w dividend_yield_1,
                LAG(dividend_yield_ranker, 4) OVER w dividend_yield_ranker_1,
                LAG(payout_ratio, 4) OVER w payout_ratio_1,
                LAG(payout_ratio_ranker, 4) OVER w payout_ratio_ranker_1,
                LAG(revenue_growth_1y, 4) OVER w revenue_growth_1y_1,
                LAG(revenue_growth_1y_ranker, 4) OVER w revenue_growth_1y_ranker_1,
                LAG(earnings_growth_1y, 4) OVER w earnings_growth_1y_1,
                LAG(earnings_growth_1y_ranker, 4) OVER w earnings_growth_1y_ranker_1,
                LAG(dividend_growth_1y, 4) OVER w dividend_growth_1y_1,
                LAG(dividend_growth_1y_ranker, 4) OVER w dividend_growth_1y_ranker_1,
                LAG(revenue_growth_3y, 4) OVER w revenue_growth_3y_1,
                LAG(revenue_growth_3y_ranker, 4) OVER w revenue_growth_3y_ranker_1,
                LAG(earnings_growth_3y, 4) OVER w earnings_growth_3y_1,
                LAG(earnings_growth_3y_ranker, 4) OVER w earnings_growth_3y_ranker_1,
                LAG(dividend_growth_3y, 4) OVER w dividend_growth_3y_1,
                LAG(dividend_growth_3y_ranker, 4) OVER w dividend_growth_3y_ranker_1,
                LAG(revenue_growth_5y, 4) OVER w revenue_growth_5y_1,
                LAG(revenue_growth_5y_ranker, 4) OVER w revenue_growth_5y_ranker_1,
                LAG(earnings_growth_5y, 4) OVER w earnings_growth_5y_1,
                LAG(earnings_growth_5y_ranker, 4) OVER w earnings_growth_5y_ranker_1,
                LAG(dividend_growth_5y, 4) OVER w dividend_growth_5y_1,
                LAG(dividend_growth_5y_ranker, 4) OVER w dividend_growth_5y_ranker_1,
                LAG(revenue_growth_9y, 4) OVER w revenue_growth_9y_1,
                LAG(revenue_growth_9y_ranker, 4) OVER w revenue_growth_9y_ranker_1,
                LAG(earnings_growth_9y, 4) OVER w earnings_growth_9y_1,
                LAG(earnings_growth_9y_ranker, 4) OVER w earnings_growth_9y_ranker_1,
                LAG(dividend_growth_9y, 4) OVER w dividend_growth_9y_1,
                LAG(dividend_growth_9y_ranker, 4) OVER w dividend_growth_9y_ranker_1,
                LAG(gross_profit_margin, 4) OVER w gross_profit_margin_1,
                LAG(gross_profit_margin_ranker, 4) OVER w gross_profit_margin_ranker_1,
                LAG(net_profit_margin, 4) OVER w net_profit_margin_1,
                LAG(net_profit_margin_ranker, 4) OVER w net_profit_margin_ranker_1,
                LAG(operating_cash_flow_margin, 4) OVER w operating_cash_flow_margin_1,
                LAG(operating_cash_flow_margin_ranker, 4) OVER w operating_cash_flow_margin_ranker_1,
                LAG(operating_margin, 4) OVER w operating_margin_1,
                LAG(operating_margin_ranker, 4) OVER w operating_margin_ranker_1,
                LAG(return_on_assets, 4) OVER w return_on_assets_1,
                LAG(return_on_assets_ranker, 4) OVER w return_on_assets_ranker_1,
                LAG(asset_turnover, 4) OVER w asset_turnover_1,
                LAG(asset_turnover_ranker, 4) OVER w asset_turnover_ranker_1,
                LAG(cash_flow_on_assets, 4) OVER w cash_flow_on_assets_1,
                LAG(cash_flow_on_assets_ranker, 4) OVER w cash_flow_on_assets_ranker_1,
                LAG(return_on_equity, 4) OVER w return_on_equity_1,
                LAG(return_on_equity_ranker, 4) OVER w return_on_equity_ranker_1,
                LAG(revenue_on_equity, 4) OVER w revenue_on_equity_1,
                LAG(revenue_on_equity_ranker, 4) OVER w revenue_on_equity_ranker_1,
                LAG(return_on_capital_employed, 4) OVER w return_on_capital_employed_1,
                LAG(return_on_capital_employed_ranker, 4) OVER w return_on_capital_employed_ranker_1,
                LAG(debt_ratio, 4) OVER w debt_ratio_1,
                LAG(debt_ratio_ranker, 4) OVER w debt_ratio_ranker_1,
                LAG(debt_to_ebit, 4) OVER w debt_to_ebit_1,
                LAG(debt_to_ebit_ranker, 4) OVER w debt_to_ebit_ranker_1,
                LAG(debt_to_equity, 4) OVER w debt_to_equity_1,
                LAG(debt_to_equity_ranker, 4) OVER w debt_to_equity_ranker_1,
                LAG(debt_to_revenue, 4) OVER w debt_to_revenue_1,
                LAG(debt_to_revenue_ranker, 4) OVER w debt_to_revenue_ranker_1,
                LAG(current_ratio, 4) OVER w current_ratio_1,
                LAG(current_ratio_ranker, 4) OVER w current_ratio_ranker_1
            FROM companies_quarterly
            WINDOW w AS (
                PARTITION BY ticker
                ORDER BY time
            )
        ) 
        UPDATE companies_quarterly c
        SET
            outstanding_shares_change = c.outstanding_shares / NULLIF(cte.outstanding_shares_1, 0),
            market_cap_change = c.market_cap / NULLIF(cte.market_cap_1, 0),
            market_cap_ranker_change = c.market_cap_ranker / NULLIF(cte.market_cap_ranker_1, 0),
            price_earnings_change = c.price_earnings / NULLIF(cte.price_earnings_1, 0),
            price_earnings_ranker_change = c.price_earnings_ranker / NULLIF(cte.price_earnings_ranker_1, 0),
            price_earnings_growth_change = c.price_earnings_growth / NULLIF(cte.price_earnings_growth_1, 0),
            price_earnings_growth_ranker_change = c.price_earnings_growth_ranker / NULLIF(cte.price_earnings_growth_ranker_1, 0),
            price_sales_change = c.price_sales / NULLIF(cte.price_sales_1, 0),
            price_sales_ranker_change = c.price_sales_ranker / NULLIF(cte.price_sales_ranker_1, 0),
            price_sales_growth_change = c.price_sales_growth / NULLIF(cte.price_sales_growth_1, 0),
            price_sales_growth_ranker_change = c.price_sales_growth_ranker / NULLIF(cte.price_sales_growth_ranker_1, 0),
            price_cash_flow_change = c.price_cash_flow / NULLIF(cte.price_cash_flow_1, 0),
            price_cash_flow_ranker_change = c.price_cash_flow_ranker / NULLIF(cte.price_cash_flow_ranker_1, 0),
            price_book_change = c.price_book / NULLIF(cte.price_book_1, 0),
            price_book_ranker_change = c.price_book_ranker / NULLIF(cte.price_book_ranker_1, 0),
            dividend_yield_change = c.dividend_yield / NULLIF(cte.dividend_yield_1, 0),
            dividend_yield_ranker_change = c.dividend_yield_ranker / NULLIF(cte.dividend_yield_ranker_1, 0),
            payout_ratio_change = c.payout_ratio / NULLIF(cte.payout_ratio_1, 0),
            payout_ratio_ranker_change = c.payout_ratio_ranker / NULLIF(cte.payout_ratio_ranker_1, 0),
            revenue_growth_1y_change = c.revenue_growth_1y / NULLIF(cte.revenue_growth_1y_1, 0),
            revenue_growth_1y_ranker_change = c.revenue_growth_1y_ranker / NULLIF(cte.revenue_growth_1y_ranker_1, 0),
            earnings_growth_1y_change = c.earnings_growth_1y / NULLIF(cte.earnings_growth_1y_1, 0),
            earnings_growth_1y_ranker_change = c.earnings_growth_1y_ranker / NULLIF(cte.earnings_growth_1y_ranker_1, 0),
            dividend_growth_1y_change = c.dividend_growth_1y / NULLIF(cte.dividend_growth_1y_1, 0),
            dividend_growth_1y_ranker_change = c.dividend_growth_1y_ranker / NULLIF(cte.dividend_growth_1y_ranker_1, 0),
            revenue_growth_3y_change = c.revenue_growth_3y / NULLIF(cte.revenue_growth_3y_1, 0),
            revenue_growth_3y_ranker_change = c.revenue_growth_3y_ranker / NULLIF(cte.revenue_growth_3y_ranker_1, 0),
            earnings_growth_3y_change = c.earnings_growth_3y / NULLIF(cte.earnings_growth_3y_1, 0),
            earnings_growth_3y_ranker_change = c.earnings_growth_3y_ranker / NULLIF(cte.earnings_growth_3y_ranker_1, 0),
            dividend_growth_3y_change = c.dividend_growth_3y / NULLIF(cte.dividend_growth_3y_1, 0),
            dividend_growth_3y_ranker_change = c.dividend_growth_3y_ranker / NULLIF(cte.dividend_growth_3y_ranker_1, 0),
            revenue_growth_5y_change = c.revenue_growth_5y / NULLIF(cte.revenue_growth_5y_1, 0),
            revenue_growth_5y_ranker_change = c.revenue_growth_5y_ranker / NULLIF(cte.revenue_growth_5y_ranker_1, 0),
            earnings_growth_5y_change = c.earnings_growth_5y / NULLIF(cte.earnings_growth_5y_1, 0),
            earnings_growth_5y_ranker_change = c.earnings_growth_5y_ranker / NULLIF(cte.earnings_growth_5y_ranker_1, 0),
            dividend_growth_5y_change = c.dividend_growth_5y / NULLIF(cte.dividend_growth_5y_1, 0),
            dividend_growth_5y_ranker_change = c.dividend_growth_5y_ranker / NULLIF(cte.dividend_growth_5y_ranker_1, 0),
            revenue_growth_9y_change = c.revenue_growth_9y / NULLIF(cte.revenue_growth_9y_1, 0),
            revenue_growth_9y_ranker_change = c.revenue_growth_9y_ranker / NULLIF(cte.revenue_growth_9y_ranker_1, 0),
            earnings_growth_9y_change = c.earnings_growth_9y / NULLIF(cte.earnings_growth_9y_1, 0),
            earnings_growth_9y_ranker_change = c.earnings_growth_9y_ranker / NULLIF(cte.earnings_growth_9y_ranker_1, 0),
            dividend_growth_9y_change = c.dividend_growth_9y / NULLIF(cte.dividend_growth_9y_1, 0),
            dividend_growth_9y_ranker_change = c.dividend_growth_9y_ranker / NULLIF(cte.dividend_growth_9y_ranker_1, 0),
            gross_profit_margin_change = c.gross_profit_margin / NULLIF(cte.gross_profit_margin_1, 0),
            gross_profit_margin_ranker_change = c.gross_profit_margin_ranker / NULLIF(cte.gross_profit_margin_ranker_1, 0),
            net_profit_margin_change = c.net_profit_margin / NULLIF(cte.net_profit_margin_1, 0),
            net_profit_margin_ranker_change = c.net_profit_margin_ranker / NULLIF(cte.net_profit_margin_ranker_1, 0),
            operating_cash_flow_margin_change = c.operating_cash_flow_margin / NULLIF(cte.operating_cash_flow_margin_1, 0),
            operating_cash_flow_margin_ranker_change = c.operating_cash_flow_margin_ranker / NULLIF(cte.operating_cash_flow_margin_ranker_1, 0),
            operating_margin_change = c.operating_margin / NULLIF(cte.operating_margin_1, 0),
            operating_margin_ranker_change = c.operating_margin_ranker / NULLIF(cte.operating_margin_ranker_1, 0),
            return_on_assets_change = c.return_on_assets / NULLIF(cte.return_on_assets_1, 0),
            return_on_assets_ranker_change = c.return_on_assets_ranker / NULLIF(cte.return_on_assets_ranker_1, 0),
            asset_turnover_change = c.asset_turnover / NULLIF(cte.asset_turnover_1, 0),
            asset_turnover_ranker_change = c.asset_turnover_ranker / NULLIF(cte.asset_turnover_ranker_1, 0),
            cash_flow_on_assets_change = c.cash_flow_on_assets / NULLIF(cte.cash_flow_on_assets_1, 0),
            cash_flow_on_assets_ranker_change = c.cash_flow_on_assets_ranker / NULLIF(cte.cash_flow_on_assets_ranker_1, 0),
            return_on_equity_change = c.return_on_equity / NULLIF(cte.return_on_equity_1, 0),
            return_on_equity_ranker_change = c.return_on_equity_ranker / NULLIF(cte.return_on_equity_ranker_1, 0),
            revenue_on_equity_change = c.revenue_on_equity / NULLIF(cte.revenue_on_equity_1, 0),
            revenue_on_equity_ranker_change = c.revenue_on_equity_ranker / NULLIF(cte.revenue_on_equity_ranker_1, 0),
            return_on_capital_employed_change = c.return_on_capital_employed / NULLIF(cte.return_on_capital_employed_1, 0),
            return_on_capital_employed_ranker_change = c.return_on_capital_employed_ranker / NULLIF(cte.return_on_capital_employed_ranker_1, 0),
            debt_ratio_change = c.debt_ratio / NULLIF(cte.debt_ratio_1, 0),
            debt_ratio_ranker_change = c.debt_ratio_ranker / NULLIF(cte.debt_ratio_ranker_1, 0),
            debt_to_ebit_change = c.debt_to_ebit / NULLIF(cte.debt_to_ebit_1, 0),
            debt_to_ebit_ranker_change = c.debt_to_ebit_ranker / NULLIF(cte.debt_to_ebit_ranker_1, 0),
            debt_to_equity_change = c.debt_to_equity / NULLIF(cte.debt_to_equity_1, 0),
            debt_to_equity_ranker_change = c.debt_to_equity_ranker / NULLIF(cte.debt_to_equity_ranker_1, 0),
            debt_to_revenue_change = c.debt_to_revenue / NULLIF(cte.debt_to_revenue_1, 0),
            debt_to_revenue_ranker_change = c.debt_to_revenue_ranker / NULLIF(cte.debt_to_revenue_ranker_1, 0),
            current_ratio_change = c.current_ratio / NULLIF(cte.current_ratio_1, 0),
            current_ratio_ranker_change = c.current_ratio_ranker / NULLIF(cte.current_ratio_ranker_1, 0)
        FROM cte
        WHERE c.ticker = cte.ticker
        AND c.time = cte.time
        ;
    '''

    db.execute(sql)
    db.commit()

    end = time()

    print(f'Finished calculating quarterly changes. ({end - start:.2f}s)')


def update_quarterly_figures(db):
    '''
    Updates all quarterly figures in db.
    '''

    calculate_ttm(db)
    calculate_metrics(db)
    calculate_growth(db)
    calculate_rankings(db)
    calculate_change(db)
