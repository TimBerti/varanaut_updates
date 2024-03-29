from time import time


def update_companies_display(db):
    '''
    Updates companies_display with values in companies_quarterly.
    '''

    print('Updating companies_display...')

    start = time()

    sql = '''
    
        -- companies_quarterly -> companies_display 
        
        WITH cte2 AS (
            WITH cte AS (
                SELECT
                    *,
                    ARRAY_AGG(time) OVER w time_arr,
                    ARRAY_AGG(outstanding_shares) OVER w outstanding_shares_arr,
                    ARRAY_AGG(total_revenue_ttm) OVER w total_revenue_arr,
                    ARRAY_AGG(free_cashflow_ttm) OVER w free_cashflow_arr,
                    ARRAY_AGG(net_income_ttm) OVER w net_income_arr,
                    ARRAY_AGG(ebit_ttm) OVER w ebit_arr,
                    ARRAY_AGG(ebitda_ttm) OVER w ebitda_arr,
                    ARRAY_AGG(gross_profit_ttm) OVER w gross_profit_arr,
                    ARRAY_AGG(dividends_paid_ttm) OVER w dividends_paid_arr,
                    ARRAY_AGG(total_liabilities) OVER w total_liabilities_arr,
                    ARRAY_AGG(total_assets) OVER w total_assets_arr,
                    ARRAY_AGG(total_current_liabilities) OVER w total_current_liabilities_arr,
                    ARRAY_AGG(total_current_assets) OVER w total_current_assets_arr,
                    ARRAY_AGG(research_development_ttm) OVER w research_development_arr,
                    ARRAY_AGG(price_earnings) OVER w price_earnings_arr,
                    ARRAY_AGG(price_ebit) OVER w price_ebit_arr,
                    ARRAY_AGG(ev_ebit) OVER w ev_ebit_arr,
                    ARRAY_AGG(ev_ebitda) OVER w ev_ebitda_arr,
                    ARRAY_AGG(price_sales) OVER w price_sales_arr,
                    ARRAY_AGG(price_book) OVER w price_book_arr,
                    ARRAY_AGG(dividend_yield) OVER w dividend_yield_arr,
                    ARRAY_AGG(payout_ratio) OVER w payout_ratio_arr,
                    ARRAY_AGG(gross_profit_margin) OVER w gross_profit_margin_arr,
                    ARRAY_AGG(net_profit_margin) OVER w net_profit_margin_arr,
                    ARRAY_AGG(operating_cash_flow_margin) OVER w operating_cash_flow_margin_arr,
                    ARRAY_AGG(operating_margin) OVER w operating_margin_arr,
                    ARRAY_AGG(return_on_assets) OVER w return_on_assets_arr,
                    ARRAY_AGG(return_on_net_tangible_assets) OVER w return_on_net_tangible_assets_arr,
                    ARRAY_AGG(asset_turnover) OVER w asset_turnover_arr,
                    ARRAY_AGG(cash_flow_on_assets) OVER w cash_flow_on_assets_arr,
                    ARRAY_AGG(return_on_equity) OVER w return_on_equity_arr,
                    ARRAY_AGG(revenue_on_equity) OVER w revenue_on_equity_arr,
                    ARRAY_AGG(return_on_capital_employed) OVER w return_on_capital_employed_arr,
                    ARRAY_AGG(return_on_capital) OVER w return_on_capital_arr,
                    ARRAY_AGG(debt_ratio) OVER w debt_ratio_arr,
                    ARRAY_AGG(debt_to_ebit) OVER w debt_to_ebit_arr,
                    ARRAY_AGG(debt_to_equity) OVER w debt_to_equity_arr,
                    ARRAY_AGG(debt_to_revenue) OVER w debt_to_revenue_arr,
                    ARRAY_AGG(current_ratio) OVER w current_ratio_arr,
                    ARRAY_AGG(price_cash_flow) OVER w price_cash_flow_arr
                FROM companies_quarterly
                WINDOW w AS (
                    PARTITION BY ticker
                    ORDER BY time
                )
            )
            SELECT DISTINCT ON (ticker)
                *
            FROM cte
            ORDER BY ticker, time DESC
        )
        INSERT INTO companies_display (
            time,
            outstanding_shares,
            outstanding_shares_change,
            total_revenue,
            free_cashflow,
            net_income,
            ebit,
            ebitda,
            gross_profit,
            dividends_paid,
            total_liabilities,
            total_current_liabilities,
            total_assets,
            total_current_assets,
            net_working_capital,
            retained_earnings,
            short_long_term_debt_total,
            cash,
            interest_expense,
            ev,
            market_cap,
            market_cap_change,
            market_cap_ranker,
            market_cap_ranker_change,
            price_earnings_change,
            price_earnings_ranker_change,
            price_ebit_change,
            price_ebit_ranker_change,
            ev_ebit_change,
            ev_ebit_ranker_change,
            ev_ebitda_change,
            ev_ebitda_ranker_change,
            price_earnings_growth,
            price_earnings_growth_change,
            price_earnings_growth_ranker,
            price_earnings_growth_ranker_change,
            price_sales_change,
            price_sales_ranker_change,
            price_sales_growth,
            price_sales_growth_change,
            price_sales_growth_ranker,
            price_sales_growth_ranker_change,
            price_cash_flow_change,
            price_cash_flow_ranker_change,
            price_book_change,
            price_book_ranker_change,
            dividend_yield,
            dividend_yield_change,
            dividend_yield_ranker,
            dividend_yield_ranker_change,
            revenue_growth_1y,
            revenue_growth_1y_change,
            revenue_growth_1y_ranker,
            revenue_growth_1y_ranker_change,
            earnings_growth_1y,
            earnings_growth_1y_change,
            earnings_growth_1y_ranker,
            earnings_growth_1y_ranker_change,
            dividend_growth_1y,
            dividend_growth_1y_change,
            dividend_growth_1y_ranker,
            dividend_growth_1y_ranker_change,
            revenue_growth_3y,
            revenue_growth_3y_change,
            revenue_growth_3y_ranker,
            revenue_growth_3y_ranker_change,
            earnings_growth_3y,
            earnings_growth_3y_change,
            earnings_growth_3y_ranker,
            earnings_growth_3y_ranker_change,
            dividend_growth_3y,
            dividend_growth_3y_change,
            dividend_growth_3y_ranker,
            dividend_growth_3y_ranker_change,
            revenue_growth_5y,
            revenue_growth_5y_change,
            revenue_growth_5y_ranker,
            revenue_growth_5y_ranker_change,
            earnings_growth_5y,
            earnings_growth_5y_change,
            earnings_growth_5y_ranker,
            earnings_growth_5y_ranker_change,
            dividend_growth_5y,
            dividend_growth_5y_change,
            dividend_growth_5y_ranker,
            dividend_growth_5y_ranker_change,
            revenue_growth_9y,
            revenue_growth_9y_change,
            revenue_growth_9y_ranker,
            revenue_growth_9y_ranker_change,
            earnings_growth_9y,
            earnings_growth_9y_change,
            earnings_growth_9y_ranker,
            earnings_growth_9y_ranker_change,
            dividend_growth_9y,
            dividend_growth_9y_change,
            dividend_growth_9y_ranker,
            dividend_growth_9y_ranker_change,
            gross_profit_margin,
            gross_profit_margin_change,
            gross_profit_margin_ranker,
            gross_profit_margin_ranker_change,
            net_profit_margin,
            net_profit_margin_change,
            net_profit_margin_ranker,
            net_profit_margin_ranker_change,
            return_on_assets,
            return_on_assets_change,
            return_on_assets_ranker,
            return_on_assets_ranker_change,
            return_on_net_tangible_assets,
            return_on_net_tangible_assets_change,
            return_on_net_tangible_assets_ranker,
            return_on_net_tangible_assets_ranker_change,
            asset_turnover,
            asset_turnover_change,
            asset_turnover_ranker,
            asset_turnover_ranker_change,
            cash_flow_on_assets,
            cash_flow_on_assets_change,
            cash_flow_on_assets_ranker,
            cash_flow_on_assets_ranker_change,
            return_on_equity,
            return_on_equity_change,
            return_on_equity_ranker,
            return_on_equity_ranker_change,
            revenue_on_equity,
            revenue_on_equity_change,
            revenue_on_equity_ranker,
            revenue_on_equity_ranker_change,
            return_on_capital_employed,
            return_on_capital_employed_change,
            return_on_capital_employed_ranker,
            return_on_capital_employed_ranker_change,
            return_on_capital,
            return_on_capital_change,
            return_on_capital_ranker,
            return_on_capital_ranker_change,
            debt_ratio,
            debt_ratio_change,
            debt_ratio_ranker,
            debt_ratio_ranker_change,
            debt_to_ebit,
            debt_to_ebit_change,
            debt_to_ebit_ranker,
            debt_to_ebit_ranker_change,
            debt_to_equity,
            debt_to_equity_change,
            debt_to_equity_ranker,
            debt_to_equity_ranker_change,
            debt_to_revenue,
            debt_to_revenue_change,
            debt_to_revenue_ranker,
            debt_to_revenue_ranker_change,
            current_ratio,
            current_ratio_change,
            current_ratio_ranker,
            current_ratio_ranker_change,
            operating_cash_flow_margin,
            operating_cash_flow_margin_change,
            operating_cash_flow_margin_ranker,
            operating_cash_flow_margin_ranker_change,
            operating_margin,
            operating_margin_change,
            operating_margin_ranker,
            operating_margin_ranker_change,
            research_development,
            payout_ratio,
            payout_ratio_change,
            payout_ratio_ranker,
            payout_ratio_ranker_change,
            time_arr,
            outstanding_shares_arr,
            total_revenue_arr,
            free_cashflow_arr,
            net_income_arr,
            ebit_arr,
            ebitda_arr,
            gross_profit_arr,
            dividends_paid_arr,
            total_liabilities_arr,
            total_assets_arr,
            total_current_liabilities_arr,
            total_current_assets_arr,
            research_development_arr,
            price_earnings_arr,
            price_ebit_arr,
            ev_ebit_arr,
            ev_ebitda_arr,
            price_sales_arr,
            price_book_arr,
            dividend_yield_arr,
            payout_ratio_arr,
            gross_profit_margin_arr,
            net_profit_margin_arr,
            operating_cash_flow_margin_arr,
            operating_margin_arr,
            return_on_assets_arr,
            return_on_net_tangible_assets_arr,
            asset_turnover_arr,
            cash_flow_on_assets_arr,
            return_on_equity_arr,
            revenue_on_equity_arr,
            return_on_capital_employed_arr,
            return_on_capital_arr,
            debt_ratio_arr,
            debt_to_ebit_arr,
            debt_to_equity_arr,
            debt_to_revenue_arr,
            current_ratio_arr,
            price_cash_flow_arr,
            ticker,
            relative_score,
            relative_score_continuous,
            piotroski_score
        )
        SELECT
            cte2.time,
            cte2.outstanding_shares,
            cte2.outstanding_shares_change,
            cte2.total_revenue_ttm,
            cte2.free_cashflow_ttm,
            cte2.net_income_ttm,
            cte2.ebit_ttm,
            cte2.ebitda_ttm,
            cte2.gross_profit_ttm,
            cte2.dividends_paid_ttm,
            cte2.total_liabilities,
            cte2.total_current_liabilities,
            cte2.total_assets,
            cte2.total_current_assets,
            cte2.net_working_capital,
            cte2.retained_earnings,
            cte2.short_long_term_debt_total,
            cte2.cash,
            cte2.interest_expense_ttm,
            cte2.ev,
            cte2.market_cap,
            cte2.market_cap_change,
            cte2.market_cap_ranker,
            cte2.market_cap_ranker_change,
            cte2.price_earnings_change,
            cte2.price_earnings_ranker_change,
            cte2.price_ebit_change,
            cte2.price_ebit_ranker_change,
            cte2.ev_ebit_change,
            cte2.ev_ebit_ranker_change,
            cte2.ev_ebitda_change,
            cte2.ev_ebitda_ranker_change,
            cte2.price_earnings_growth,
            cte2.price_earnings_growth_change,
            cte2.price_earnings_growth_ranker,
            cte2.price_earnings_growth_ranker_change,
            cte2.price_sales_change,
            cte2.price_sales_ranker_change,
            cte2.price_sales_growth,
            cte2.price_sales_growth_change,
            cte2.price_sales_growth_ranker,
            cte2.price_sales_growth_ranker_change,
            cte2.price_cash_flow_change,
            cte2.price_cash_flow_ranker_change,
            cte2.price_book_change,
            cte2.price_book_ranker_change,
            cte2.dividend_yield,
            cte2.dividend_yield_change,
            cte2.dividend_yield_ranker,
            cte2.dividend_yield_ranker_change,
            cte2.revenue_growth_1y,
            cte2.revenue_growth_1y_change,
            cte2.revenue_growth_1y_ranker,
            cte2.revenue_growth_1y_ranker_change,
            cte2.earnings_growth_1y,
            cte2.earnings_growth_1y_change,
            cte2.earnings_growth_1y_ranker,
            cte2.earnings_growth_1y_ranker_change,
            cte2.dividend_growth_1y,
            cte2.dividend_growth_1y_change,
            cte2.dividend_growth_1y_ranker,
            cte2.dividend_growth_1y_ranker_change,
            cte2.revenue_growth_3y,
            cte2.revenue_growth_3y_change,
            cte2.revenue_growth_3y_ranker,
            cte2.revenue_growth_3y_ranker_change,
            cte2.earnings_growth_3y,
            cte2.earnings_growth_3y_change,
            cte2.earnings_growth_3y_ranker,
            cte2.earnings_growth_3y_ranker_change,
            cte2.dividend_growth_3y,
            cte2.dividend_growth_3y_change,
            cte2.dividend_growth_3y_ranker,
            cte2.dividend_growth_3y_ranker_change,
            cte2.revenue_growth_5y,
            cte2.revenue_growth_5y_change,
            cte2.revenue_growth_5y_ranker,
            cte2.revenue_growth_5y_ranker_change,
            cte2.earnings_growth_5y,
            cte2.earnings_growth_5y_change,
            cte2.earnings_growth_5y_ranker,
            cte2.earnings_growth_5y_ranker_change,
            cte2.dividend_growth_5y,
            cte2.dividend_growth_5y_change,
            cte2.dividend_growth_5y_ranker,
            cte2.dividend_growth_5y_ranker_change,
            cte2.revenue_growth_9y,
            cte2.revenue_growth_9y_change,
            cte2.revenue_growth_9y_ranker,
            cte2.revenue_growth_9y_ranker_change,
            cte2.earnings_growth_9y,
            cte2.earnings_growth_9y_change,
            cte2.earnings_growth_9y_ranker,
            cte2.earnings_growth_9y_ranker_change,
            cte2.dividend_growth_9y,
            cte2.dividend_growth_9y_change,
            cte2.dividend_growth_9y_ranker,
            cte2.dividend_growth_9y_ranker_change,
            cte2.gross_profit_margin,
            cte2.gross_profit_margin_change,
            cte2.gross_profit_margin_ranker,
            cte2.gross_profit_margin_ranker_change,
            cte2.net_profit_margin,
            cte2.net_profit_margin_change,
            cte2.net_profit_margin_ranker,
            cte2.net_profit_margin_ranker_change,
            cte2.return_on_assets,
            cte2.return_on_assets_change,
            cte2.return_on_assets_ranker,
            cte2.return_on_assets_ranker_change,
            cte2.return_on_net_tangible_assets,
            cte2.return_on_net_tangible_assets_change,
            cte2.return_on_net_tangible_assets_ranker,
            cte2.return_on_net_tangible_assets_ranker_change,
            cte2.asset_turnover,
            cte2.asset_turnover_change,
            cte2.asset_turnover_ranker,
            cte2.asset_turnover_ranker_change,
            cte2.cash_flow_on_assets,
            cte2.cash_flow_on_assets_change,
            cte2.cash_flow_on_assets_ranker,
            cte2.cash_flow_on_assets_ranker_change,
            cte2.return_on_equity,
            cte2.return_on_equity_change,
            cte2.return_on_equity_ranker,
            cte2.return_on_equity_ranker_change,
            cte2.revenue_on_equity,
            cte2.revenue_on_equity_change,
            cte2.revenue_on_equity_ranker,
            cte2.revenue_on_equity_ranker_change,
            cte2.return_on_capital_employed,
            cte2.return_on_capital_employed_change,
            cte2.return_on_capital_employed_ranker,
            cte2.return_on_capital_employed_ranker_change,
            cte2.return_on_capital,
            cte2.return_on_capital_change,
            cte2.return_on_capital_ranker,
            cte2.return_on_capital_ranker_change,
            cte2.debt_ratio,
            cte2.debt_ratio_change,
            cte2.debt_ratio_ranker,
            cte2.debt_ratio_ranker_change,
            cte2.debt_to_ebit,
            cte2.debt_to_ebit_change,
            cte2.debt_to_ebit_ranker,
            cte2.debt_to_ebit_ranker_change,
            cte2.debt_to_equity,
            cte2.debt_to_equity_change,
            cte2.debt_to_equity_ranker,
            cte2.debt_to_equity_ranker_change,
            cte2.debt_to_revenue,
            cte2.debt_to_revenue_change,
            cte2.debt_to_revenue_ranker,
            cte2.debt_to_revenue_ranker_change,
            cte2.current_ratio,
            cte2.current_ratio_change,
            cte2.current_ratio_ranker,
            cte2.current_ratio_ranker_change,
            cte2.operating_cash_flow_margin,
            cte2.operating_cash_flow_margin_change,
            cte2.operating_cash_flow_margin_ranker,
            cte2.operating_cash_flow_margin_ranker_change,
            cte2.operating_margin,
            cte2.operating_margin_change,
            cte2.operating_margin_ranker,
            cte2.operating_margin_ranker_change,
            cte2.research_development,
            cte2.payout_ratio,
            cte2.payout_ratio_change,
            cte2.payout_ratio_ranker,
            cte2.payout_ratio_ranker_change,
            cte2.time_arr,
            cte2.outstanding_shares_arr,
            cte2.total_revenue_arr,
            cte2.free_cashflow_arr,
            cte2.net_income_arr,
            cte2.ebit_arr,
            cte2.ebitda_arr,
            cte2.gross_profit_arr,
            cte2.dividends_paid_arr,
            cte2.total_liabilities_arr,
            cte2.total_assets_arr,
            cte2.total_current_liabilities_arr,
            cte2.total_current_assets_arr,
            cte2.research_development_arr,
            cte2.price_earnings_arr,
            cte2.price_ebit_arr,
            cte2.ev_ebit_arr,
            cte2.ev_ebitda_arr,
            cte2.price_sales_arr,
            cte2.price_book_arr,
            cte2.dividend_yield_arr,
            cte2.payout_ratio_arr,
            cte2.gross_profit_margin_arr,
            cte2.net_profit_margin_arr,
            cte2.operating_cash_flow_margin_arr,
            cte2.operating_margin_arr,
            cte2.return_on_assets_arr,
            cte2.return_on_net_tangible_assets_arr,
            cte2.asset_turnover_arr,
            cte2.cash_flow_on_assets_arr,
            cte2.return_on_equity_arr,
            cte2.revenue_on_equity_arr,
            cte2.return_on_capital_employed_arr,
            cte2.return_on_capital_arr,
            cte2.debt_ratio_arr,
            cte2.debt_to_ebit_arr,
            cte2.debt_to_equity_arr,
            cte2.debt_to_revenue_arr,
            cte2.current_ratio_arr,
            cte2.price_cash_flow_arr,
            cte2.ticker,
            cte2.relative_score,
            cte2.relative_score_continuous,
            cte2.piotroski_score
        FROM cte2
        ON CONFLICT(ticker) DO
        UPDATE
        SET
            time = EXCLUDED.time,
            outstanding_shares = EXCLUDED.outstanding_shares,
            outstanding_shares_change = EXCLUDED.outstanding_shares_change,
            total_revenue = EXCLUDED.total_revenue,
            free_cashflow = EXCLUDED.free_cashflow,
            net_income = EXCLUDED.net_income,
            ebit = EXCLUDED.ebit,
            ebitda = EXCLUDED.ebitda,
            gross_profit = EXCLUDED.gross_profit,
            dividends_paid = EXCLUDED.dividends_paid,
            total_liabilities = EXCLUDED.total_liabilities,
            total_current_liabilities = EXCLUDED.total_current_liabilities,
            total_assets = EXCLUDED.total_assets,
            total_current_assets = EXCLUDED.total_current_assets,
            net_working_capital = EXCLUDED.net_working_capital,
            retained_earnings = EXCLUDED.retained_earnings,
            short_long_term_debt_total = EXCLUDED.short_long_term_debt_total,
            cash = EXCLUDED.cash,
            interest_expense = EXCLUDED.interest_expense,
            ev = EXCLUDED.ev,
            market_cap = EXCLUDED.market_cap,
            market_cap_change = EXCLUDED.market_cap_change,
            market_cap_ranker = EXCLUDED.market_cap_ranker,
            market_cap_ranker_change = EXCLUDED.market_cap_ranker_change,
            price_earnings_change = EXCLUDED.price_earnings_change,
            price_earnings_ranker_change = EXCLUDED.price_earnings_ranker_change,
            price_ebit_change = EXCLUDED.price_ebit_change,
            price_ebit_ranker_change = EXCLUDED.price_ebit_ranker_change,
            ev_ebit_change = EXCLUDED.ev_ebit_change,
            ev_ebit_ranker_change = EXCLUDED.ev_ebit_ranker_change,
            ev_ebitda_change = EXCLUDED.ev_ebitda_change,
            ev_ebitda_ranker_change = EXCLUDED.ev_ebitda_ranker_change,
            price_earnings_growth = EXCLUDED.price_earnings_growth,
            price_earnings_growth_change = EXCLUDED.price_earnings_growth_change,
            price_earnings_growth_ranker = EXCLUDED.price_earnings_growth_ranker,
            price_earnings_growth_ranker_change = EXCLUDED.price_earnings_growth_ranker_change,
            price_sales_change = EXCLUDED.price_sales_change,
            price_sales_ranker_change = EXCLUDED.price_sales_ranker_change,
            price_sales_growth = EXCLUDED.price_sales_growth,
            price_sales_growth_change = EXCLUDED.price_sales_growth_change,
            price_sales_growth_ranker = EXCLUDED.price_sales_growth_ranker,
            price_sales_growth_ranker_change = EXCLUDED.price_sales_growth_ranker_change,
            price_cash_flow_change = EXCLUDED.price_cash_flow_change,
            price_cash_flow_ranker_change = EXCLUDED.price_cash_flow_ranker_change,
            price_book_change = EXCLUDED.price_book_change,
            price_book_ranker_change = EXCLUDED.price_book_ranker_change,
            dividend_yield = EXCLUDED.dividend_yield,
            dividend_yield_change = EXCLUDED.dividend_yield_change,
            dividend_yield_ranker = EXCLUDED.dividend_yield_ranker,
            dividend_yield_ranker_change = EXCLUDED.dividend_yield_ranker_change,
            revenue_growth_1y = EXCLUDED.revenue_growth_1y,
            revenue_growth_1y_change = EXCLUDED.revenue_growth_1y_change,
            revenue_growth_1y_ranker = EXCLUDED.revenue_growth_1y_ranker,
            revenue_growth_1y_ranker_change = EXCLUDED.revenue_growth_1y_ranker_change,
            earnings_growth_1y = EXCLUDED.earnings_growth_1y,
            earnings_growth_1y_change = EXCLUDED.earnings_growth_1y_change,
            earnings_growth_1y_ranker = EXCLUDED.earnings_growth_1y_ranker,
            earnings_growth_1y_ranker_change = EXCLUDED.earnings_growth_1y_ranker_change,
            dividend_growth_1y = EXCLUDED.dividend_growth_1y,
            dividend_growth_1y_change = EXCLUDED.dividend_growth_1y_change,
            dividend_growth_1y_ranker = EXCLUDED.dividend_growth_1y_ranker,
            dividend_growth_1y_ranker_change = EXCLUDED.dividend_growth_1y_ranker_change,
            revenue_growth_3y = EXCLUDED.revenue_growth_3y,
            revenue_growth_3y_change = EXCLUDED.revenue_growth_3y_change,
            revenue_growth_3y_ranker = EXCLUDED.revenue_growth_3y_ranker,
            revenue_growth_3y_ranker_change = EXCLUDED.revenue_growth_3y_ranker_change,
            earnings_growth_3y = EXCLUDED.earnings_growth_3y,
            earnings_growth_3y_change = EXCLUDED.earnings_growth_3y_change,
            earnings_growth_3y_ranker = EXCLUDED.earnings_growth_3y_ranker,
            earnings_growth_3y_ranker_change = EXCLUDED.earnings_growth_3y_ranker_change,
            dividend_growth_3y = EXCLUDED.dividend_growth_3y,
            dividend_growth_3y_change = EXCLUDED.dividend_growth_3y_change,
            dividend_growth_3y_ranker = EXCLUDED.dividend_growth_3y_ranker,
            dividend_growth_3y_ranker_change = EXCLUDED.dividend_growth_3y_ranker_change,
            revenue_growth_5y = EXCLUDED.revenue_growth_5y,
            revenue_growth_5y_change = EXCLUDED.revenue_growth_5y_change,
            revenue_growth_5y_ranker = EXCLUDED.revenue_growth_5y_ranker,
            revenue_growth_5y_ranker_change = EXCLUDED.revenue_growth_5y_ranker_change,
            earnings_growth_5y = EXCLUDED.earnings_growth_5y,
            earnings_growth_5y_change = EXCLUDED.earnings_growth_5y_change,
            earnings_growth_5y_ranker = EXCLUDED.earnings_growth_5y_ranker,
            earnings_growth_5y_ranker_change = EXCLUDED.earnings_growth_5y_ranker_change,
            dividend_growth_5y = EXCLUDED.dividend_growth_5y,
            dividend_growth_5y_change = EXCLUDED.dividend_growth_5y_change,
            dividend_growth_5y_ranker = EXCLUDED.dividend_growth_5y_ranker,
            dividend_growth_5y_ranker_change = EXCLUDED.dividend_growth_5y_ranker_change,
            revenue_growth_9y = EXCLUDED.revenue_growth_9y,
            revenue_growth_9y_change = EXCLUDED.revenue_growth_9y_change,
            revenue_growth_9y_ranker = EXCLUDED.revenue_growth_9y_ranker,
            revenue_growth_9y_ranker_change = EXCLUDED.revenue_growth_9y_ranker_change,
            earnings_growth_9y = EXCLUDED.earnings_growth_9y,
            earnings_growth_9y_change = EXCLUDED.earnings_growth_9y_change,
            earnings_growth_9y_ranker = EXCLUDED.earnings_growth_9y_ranker,
            earnings_growth_9y_ranker_change = EXCLUDED.earnings_growth_9y_ranker_change,
            dividend_growth_9y = EXCLUDED.dividend_growth_9y,
            dividend_growth_9y_change = EXCLUDED.dividend_growth_9y_change,
            dividend_growth_9y_ranker = EXCLUDED.dividend_growth_9y_ranker,
            dividend_growth_9y_ranker_change = EXCLUDED.dividend_growth_9y_ranker_change,
            gross_profit_margin = EXCLUDED.gross_profit_margin,
            gross_profit_margin_change = EXCLUDED.gross_profit_margin_change,
            gross_profit_margin_ranker = EXCLUDED.gross_profit_margin_ranker,
            gross_profit_margin_ranker_change = EXCLUDED.gross_profit_margin_ranker_change,
            net_profit_margin = EXCLUDED.net_profit_margin,
            net_profit_margin_change = EXCLUDED.net_profit_margin_change,
            net_profit_margin_ranker = EXCLUDED.net_profit_margin_ranker,
            net_profit_margin_ranker_change = EXCLUDED.net_profit_margin_ranker_change,
            return_on_assets = EXCLUDED.return_on_assets,
            return_on_assets_change = EXCLUDED.return_on_assets_change,
            return_on_assets_ranker = EXCLUDED.return_on_assets_ranker,
            return_on_assets_ranker_change = EXCLUDED.return_on_assets_ranker_change,
            return_on_net_tangible_assets = EXCLUDED.return_on_net_tangible_assets,
            return_on_net_tangible_assets_change = EXCLUDED.return_on_net_tangible_assets_change,
            return_on_net_tangible_assets_ranker = EXCLUDED.return_on_net_tangible_assets_ranker,
            return_on_net_tangible_assets_ranker_change = EXCLUDED.return_on_net_tangible_assets_ranker_change,
            asset_turnover = EXCLUDED.asset_turnover,
            asset_turnover_change = EXCLUDED.asset_turnover_change,
            asset_turnover_ranker = EXCLUDED.asset_turnover_ranker,
            asset_turnover_ranker_change = EXCLUDED.asset_turnover_ranker_change,
            cash_flow_on_assets = EXCLUDED.cash_flow_on_assets,
            cash_flow_on_assets_change = EXCLUDED.cash_flow_on_assets_change,
            cash_flow_on_assets_ranker = EXCLUDED.cash_flow_on_assets_ranker,
            cash_flow_on_assets_ranker_change = EXCLUDED.cash_flow_on_assets_ranker_change,
            return_on_equity = EXCLUDED.return_on_equity,
            return_on_equity_change = EXCLUDED.return_on_equity_change,
            return_on_equity_ranker = EXCLUDED.return_on_equity_ranker,
            return_on_equity_ranker_change = EXCLUDED.return_on_equity_ranker_change,
            revenue_on_equity = EXCLUDED.revenue_on_equity,
            revenue_on_equity_change = EXCLUDED.revenue_on_equity_change,
            revenue_on_equity_ranker = EXCLUDED.revenue_on_equity_ranker,
            revenue_on_equity_ranker_change = EXCLUDED.revenue_on_equity_ranker_change,
            return_on_capital_employed = EXCLUDED.return_on_capital_employed,
            return_on_capital_employed_change = EXCLUDED.return_on_capital_employed_change,
            return_on_capital_employed_ranker = EXCLUDED.return_on_capital_employed_ranker,
            return_on_capital_employed_ranker_change = EXCLUDED.return_on_capital_employed_ranker_change,
            return_on_capital = EXCLUDED.return_on_capital,
            return_on_capital_change = EXCLUDED.return_on_capital_change,
            return_on_capital_ranker = EXCLUDED.return_on_capital_ranker,
            return_on_capital_ranker_change = EXCLUDED.return_on_capital_ranker_change,
            debt_ratio = EXCLUDED.debt_ratio,
            debt_ratio_change = EXCLUDED.debt_ratio_change,
            debt_ratio_ranker = EXCLUDED.debt_ratio_ranker,
            debt_ratio_ranker_change = EXCLUDED.debt_ratio_ranker_change,
            debt_to_ebit = EXCLUDED.debt_to_ebit,
            debt_to_ebit_change = EXCLUDED.debt_to_ebit_change,
            debt_to_ebit_ranker = EXCLUDED.debt_to_ebit_ranker,
            debt_to_ebit_ranker_change = EXCLUDED.debt_to_ebit_ranker_change,
            debt_to_equity = EXCLUDED.debt_to_equity,
            debt_to_equity_change = EXCLUDED.debt_to_equity_change,
            debt_to_equity_ranker = EXCLUDED.debt_to_equity_ranker,
            debt_to_equity_ranker_change = EXCLUDED.debt_to_equity_ranker_change,
            debt_to_revenue = EXCLUDED.debt_to_revenue,
            debt_to_revenue_change = EXCLUDED.debt_to_revenue_change,
            debt_to_revenue_ranker = EXCLUDED.debt_to_revenue_ranker,
            debt_to_revenue_ranker_change = EXCLUDED.debt_to_revenue_ranker_change,
            current_ratio = EXCLUDED.current_ratio,
            current_ratio_change = EXCLUDED.current_ratio_change,
            current_ratio_ranker = EXCLUDED.current_ratio_ranker,
            current_ratio_ranker_change = EXCLUDED.current_ratio_ranker_change,
            operating_cash_flow_margin = EXCLUDED.operating_cash_flow_margin,
            operating_cash_flow_margin_change = EXCLUDED.operating_cash_flow_margin_change,
            operating_cash_flow_margin_ranker = EXCLUDED.operating_cash_flow_margin_ranker,
            operating_cash_flow_margin_ranker_change = EXCLUDED.operating_cash_flow_margin_ranker_change,
            operating_margin = EXCLUDED.operating_margin,
            operating_margin_change = EXCLUDED.operating_margin_change,
            operating_margin_ranker = EXCLUDED.operating_margin_ranker,
            operating_margin_ranker_change = EXCLUDED.operating_margin_ranker_change,
            research_development = EXCLUDED.research_development,
            payout_ratio = EXCLUDED.payout_ratio,
            payout_ratio_change = EXCLUDED.payout_ratio_change,
            payout_ratio_ranker = EXCLUDED.payout_ratio_ranker,
            payout_ratio_ranker_change = EXCLUDED.payout_ratio_ranker_change,
            time_arr = EXCLUDED.time_arr,
            outstanding_shares_arr = EXCLUDED.outstanding_shares_arr,
            total_revenue_arr = EXCLUDED.total_revenue_arr,
            free_cashflow_arr = EXCLUDED.free_cashflow_arr,
            net_income_arr = EXCLUDED.net_income_arr,
            ebit_arr = EXCLUDED.ebit_arr,
            ebitda_arr = EXCLUDED.ebitda_arr,
            gross_profit_arr = EXCLUDED.gross_profit_arr,
            dividends_paid_arr = EXCLUDED.dividends_paid_arr,
            total_liabilities_arr = EXCLUDED.total_liabilities_arr,
            total_assets_arr = EXCLUDED.total_assets_arr,
            total_current_liabilities_arr = EXCLUDED.total_current_liabilities_arr,
            total_current_assets_arr = EXCLUDED.total_current_assets_arr,
            research_development_arr = EXCLUDED.research_development_arr,
            price_earnings_arr = EXCLUDED.price_earnings_arr,
            price_ebit_arr = EXCLUDED.price_ebit_arr,
            ev_ebit_arr = EXCLUDED.ev_ebit_arr,
            ev_ebitda_arr = EXCLUDED.ev_ebitda_arr,
            price_sales_arr = EXCLUDED.price_sales_arr,
            price_book_arr = EXCLUDED.price_book_arr,
            dividend_yield_arr = EXCLUDED.dividend_yield_arr,
            payout_ratio_arr = EXCLUDED.payout_ratio_arr,
            gross_profit_margin_arr = EXCLUDED.gross_profit_margin_arr,
            net_profit_margin_arr = EXCLUDED.net_profit_margin_arr,
            operating_cash_flow_margin_arr = EXCLUDED.operating_cash_flow_margin_arr,
            operating_margin_arr = EXCLUDED.operating_margin_arr,
            return_on_assets_arr = EXCLUDED.return_on_assets_arr,
            return_on_net_tangible_assets_arr = EXCLUDED.return_on_net_tangible_assets_arr,
            asset_turnover_arr = EXCLUDED.asset_turnover_arr,
            cash_flow_on_assets_arr = EXCLUDED.cash_flow_on_assets_arr,
            return_on_equity_arr = EXCLUDED.return_on_equity_arr,
            revenue_on_equity_arr = EXCLUDED.revenue_on_equity_arr,
            return_on_capital_employed_arr = EXCLUDED.return_on_capital_employed_arr,
            return_on_capital_arr = EXCLUDED.return_on_capital_arr,
            debt_ratio_arr = EXCLUDED.debt_ratio_arr,
            debt_to_ebit_arr = EXCLUDED.debt_to_ebit_arr,
            debt_to_equity_arr = EXCLUDED.debt_to_equity_arr,
            debt_to_revenue_arr = EXCLUDED.debt_to_revenue_arr,
            current_ratio_arr = EXCLUDED.current_ratio_arr,
            price_cash_flow_arr = EXCLUDED.price_cash_flow_arr,
            ticker = EXCLUDED.ticker,
            relative_score = EXCLUDED.relative_score,
            relative_score_continuous = EXCLUDED.relative_score_continuous,
            piotroski_score = EXCLUDED.piotroski_score
        ;
        -- Market Cap & Misc

        WITH cte AS (
            WITH cte2 AS (
                SELECT
                    ticker,
                    outstanding_shares
                FROM companies_quarterly
                WHERE outstanding_shares IS NOT NULL
                AND time > CURRENT_DATE - INTERVAL '1 year'
                ORDER BY time DESC
            )
            SELECT DISTINCT ON (ticker)
            *
            FROM cte2
        )
        UPDATE companies_display c
        SET 
            outstanding_shares = cte.outstanding_shares,
            market_cap = CASE WHEN stock_price * cte.outstanding_shares < 100000000000 OR stock_price * cte.outstanding_shares / total_revenue < 100 THEN stock_price * cte.outstanding_shares ELSE NULL END,
            market_cap_usd = CASE WHEN stock_price * cte.outstanding_shares < 100000000000 OR stock_price * cte.outstanding_shares / total_revenue < 100 THEN stock_price * cte.outstanding_shares ELSE NULL END,
            esg = c.ticker IN (
                SELECT UNNEST(holdings) FROM etf WHERE ticker = 'ESGV'
            )
        FROM cte
        WHERE c.ticker = cte.ticker
        ;
        
        UPDATE companies_display
        SET
            ev = market_cap + short_long_term_debt_total - cash
        ;
        
        -- Market Cap Ratios

        UPDATE companies_display
        SET 
            ev_ebit = (CASE WHEN ebit > 0 THEN ev ELSE NULL END) / NULLIF(ebit, 0),
            ev_ebitda = (CASE WHEN ebitda > 0 THEN ev ELSE NULL END) / NULLIF(ebitda, 0),
            price_earnings = CASE WHEN net_income > 0 THEN market_cap / NULLIF(net_income, 0) ELSE NULL END,
            price_ebit = CASE WHEN ebit > 0 THEN market_cap / NULLIF(ebit, 0) ELSE NULL END,
            price_sales = market_cap / NULLIF(total_revenue, 0),
            price_book = CASE WHEN total_assets - total_liabilities > 0 THEN market_cap / NULLIF(total_assets - total_liabilities, 0) ELSE NULL END,
            price_cash_flow = CASE WHEN free_cashflow > 0 THEN market_cap / NULLIF(free_cashflow, 0) ELSE NULL END,
            dividend_yield = 100 * ABS(dividends_paid) / NULLIF(market_cap, 0)
        ;

        -- Market Cap Ratio Ranker

        WITH cte AS (
            SELECT
                ticker,
                PERCENT_RANK() OVER (
                    PARTITION BY (implied_volatility IS NOT NULL)
                    ORDER BY implied_volatility DESC
                ) AS implied_volatility_ranker,
                PERCENT_RANK() OVER (
                    PARTITION BY (price_earnings IS NOT NULL)
                    ORDER BY price_earnings DESC
                ) AS price_earnings_ranker,
                PERCENT_RANK() OVER (
                    PARTITION BY (price_ebit IS NOT NULL)
                    ORDER BY price_ebit DESC
                ) AS price_ebit_ranker,
                PERCENT_RANK() OVER (
                    PARTITION BY (price_sales IS NOT NULL)
                    ORDER BY price_sales DESC
                ) AS price_sales_ranker,
                PERCENT_RANK() OVER (
                    PARTITION BY (price_book IS NOT NULL)
                    ORDER BY price_book DESC
                ) AS price_book_ranker,
                PERCENT_RANK() OVER (
                    PARTITION BY (price_cash_flow IS NOT NULL)
                    ORDER BY price_cash_flow DESC
                ) AS price_cash_flow_ranker,
                PERCENT_RANK() OVER (
                    PARTITION BY (ev_ebit IS NOT NULL)
                    ORDER BY ev_ebit DESC
                ) AS ev_ebit_ranker,
                PERCENT_RANK() OVER (
                    PARTITION BY (ev_ebitda IS NOT NULL)
                    ORDER BY ev_ebitda DESC
                ) AS ev_ebitda_ranker,
                PERCENT_RANK() OVER (
                    PARTITION BY (dividend_yield IS NOT NULL)
                    ORDER BY dividend_yield
                ) AS dividend_yield_ranker
            FROM companies_display
        )
        UPDATE companies_display c
        SET 
            implied_volatility_ranker = cte.implied_volatility_ranker,
            price_earnings_ranker = cte.price_earnings_ranker,
            price_ebit_ranker = cte.price_ebit_ranker,
            price_sales_ranker = cte.price_sales_ranker,
            price_book_ranker = cte.price_book_ranker,
            price_cash_flow_ranker = cte.price_cash_flow_ranker,
            ev_ebit_ranker = cte.ev_ebit_ranker,
            ev_ebitda_ranker = cte.ev_ebitda_ranker,
            dividend_yield_ranker = cte.dividend_yield_ranker
        FROM cte
        WHERE c.ticker = cte.ticker;

        -- Price Ratio Average & Deviation

        WITH cte AS (
            SELECT 
                ticker,
                AVG(price_earnings) AS price_earnings_average,
                AVG(price_ebit) AS price_ebit_average,
                AVG(ev_ebit) AS ev_ebit_average,
                AVG(ev_ebitda) AS ev_ebitda_average,
                AVG(price_book) AS price_book_average,
                AVG(price_sales) AS price_sales_average,
                AVG(price_cash_flow) AS price_cash_flow_average
            FROM companies_quarterly
            WHERE time >= CURRENT_DATE - INTERVAL '10 years'
            GROUP BY ticker
        )
        UPDATE companies_display c
        SET 
            price_earnings_average_10y = cte.price_earnings_average,
            price_earnings_deviation_10y = (c.price_earnings / NULLIF(cte.price_earnings_average, 0) - 1) * 100,
            price_ebit_average_10y = cte.price_ebit_average,
            price_ebit_deviation_10y = (c.price_ebit / NULLIF(cte.price_ebit_average, 0) - 1) * 100,
            ev_ebit_average_10y = cte.ev_ebit_average,
            ev_ebit_deviation_10y = (c.ev_ebit / NULLIF(cte.ev_ebit_average, 0) - 1) * 100,
            ev_ebitda_average_10y = cte.ev_ebitda_average,
            ev_ebitda_deviation_10y = (c.ev_ebitda / NULLIF(cte.ev_ebitda_average, 0) - 1) * 100,
            price_book_average_10y = cte.price_book_average,
            price_book_deviation_10y = (c.price_book / NULLIF(cte.price_book_average, 0) - 1) * 100,
            price_sales_average_10y = cte.price_sales_average,
            price_sales_deviation_10y = (c.price_sales / NULLIF(cte.price_sales_average, 0) - 1) * 100,
            price_cash_flow_average_10y = cte.price_cash_flow_average,
            price_cash_flow_deviation_10y = (c.price_cash_flow / NULLIF(cte.price_cash_flow_average, 0) - 1) * 100
        FROM cte
        WHERE cte.ticker = c.ticker
        ;
        
        -- WACC
        
        CREATE OR REPLACE FUNCTION risk_free_rate ()
        RETURNS float8 AS $risk_free_rate$
        DECLARE
            risk_free_rate float8;
        BEGIN
            SELECT adjusted_close INTO risk_free_rate
            FROM eod WHERE ticker = 'US10Y.GBOND' ORDER BY time DESC LIMIT 1;
            RETURN risk_free_rate;
        END;
        $risk_free_rate$ LANGUAGE plpgsql;

        UPDATE companies_display SET 
            wacc = (risk_free_rate() + beta * (8 - risk_free_rate())) * market_cap / NULLIF(market_cap + ABS(CASE WHEN short_long_term_debt_total IS NULL THEN 0 ELSE short_long_term_debt_total END), 0)
            + ABS(CASE WHEN interest_expense IS NULL THEN 0 ELSE interest_expense END) / NULLIF(market_cap + ABS(CASE WHEN short_long_term_debt_total IS NULL THEN 0 ELSE short_long_term_debt_total END), 0) * 100
        ;

        -- Scores

        UPDATE companies_display
        SET
            relative_score = (
                -- Valuation
                CASE WHEN price_book_ranker > 2.0/3 THEN 1 ELSE 0 END + 
                CASE WHEN price_sales_ranker > 2.0/3 THEN 1 ELSE 0 END + 
                -- Profitability
                CASE WHEN asset_turnover_ranker > 2.0/3 THEN 1 ELSE 0 END + 
                CASE WHEN gross_profit_margin_ranker > 2.0/3 THEN 1 ELSE 0 END + 
                -- Growth
                CASE WHEN revenue_growth_1y_ranker > 2.0/3 THEN 1 ELSE 0 END + 
                CASE WHEN revenue_growth_3y_ranker > 2.0/3 THEN 1 ELSE 0 END + 
                -- Debt
                CASE WHEN debt_to_equity_ranker > 2.0/3 THEN 1 ELSE 0 END + 
                CASE WHEN current_ratio_ranker > 2.0/3 THEN 1 ELSE 0 END 
            ),
            relative_score_continuous = (
                -- Valuation
                CASE WHEN price_book_ranker IS NOT NULL THEN price_book_ranker ELSE 0 END + 
                CASE WHEN price_sales_ranker IS NOT NULL THEN price_sales_ranker ELSE 0 END + 
                -- Profitability
                CASE WHEN asset_turnover_ranker IS NOT NULL THEN asset_turnover_ranker ELSE 0 END + 
                CASE WHEN gross_profit_margin_ranker IS NOT NULL THEN gross_profit_margin_ranker ELSE 0 END + 
                -- Growth
                CASE WHEN revenue_growth_1y_ranker IS NOT NULL THEN revenue_growth_1y_ranker ELSE 0 END + 
                CASE WHEN revenue_growth_3y_ranker IS NOT NULL THEN revenue_growth_3y_ranker ELSE 0 END + 
                -- Debt
                CASE WHEN debt_to_equity_ranker IS NOT NULL THEN debt_to_equity_ranker ELSE 0 END + 
                CASE WHEN current_ratio_ranker IS NOT NULL THEN current_ratio_ranker ELSE 0 END 
            ),
            piotroski_score = (
                -- Profitability
                CASE WHEN return_on_assets > 0 THEN 1 ELSE 0 END + 
                CASE WHEN free_cashflow > 0 THEN 1 ELSE 0 END + 
                CASE WHEN return_on_assets_change >= 1 THEN 1 ELSE 0 END + 
                CASE WHEN free_cashflow / NULLIF(total_assets, 0) * 100 > return_on_assets THEN 1 ELSE 0 END +
                -- Leverage, Liquidity and Source of Funds 
                CASE WHEN debt_to_equity_change <= 1 THEN 1 ELSE 0 END + 
                CASE WHEN current_ratio_change >= 1 THEN 1 ELSE 0 END + 
                CASE WHEN outstanding_shares_change <= 1 THEN 1 ELSE 0 END + 
                -- Operating Efficiency
                CASE WHEN gross_profit_margin_change >= 1 THEN 1 ELSE 0 END + 
                CASE WHEN asset_turnover_change >= 1 THEN 1 ELSE 0 END
            ),
            combined_score = relative_score_continuous / 8 * rsi_180 / 100,
            altman_z_score = (
                1.2 * net_working_capital / NULLIF(total_assets, 0) + 
                1.4 * retained_earnings / NULLIF(total_assets, 0) + 
                3.3 * ebit / NULLIF(total_assets, 0) + 
                0.6 * market_cap / NULLIF(total_liabilities, 0) + 
                1 * total_revenue / NULLIF(total_assets, 0)
            )
        ;
            
        -- Volume deviation
        
        WITH cte AS (
            WITH RECURSIVE
            annual AS (
                SELECT 
                    ticker, 
                    AVG(volume) AS avg_volume
                FROM eod 
                WHERE time > CURRENT_DATE - INTERVAL '1 year'
                AND volume IS NOT NULL
                GROUP BY ticker
            ),
            weekly AS (
                SELECT 
                    ticker, 
                    AVG(volume) AS avg_volume
                FROM eod 
                WHERE time > CURRENT_DATE - INTERVAL '1 week'
                AND volume IS NOT NULL
                GROUP BY ticker
            )
            SELECT
                w.ticker,
                (w.avg_volume / NULLIF(a.avg_volume, 0) - 1) * 100 AS volume_deviation
            FROM weekly w
            JOIN annual a
            ON w.ticker = a.ticker
        )
        UPDATE companies_display c
        SET
            volume_deviation = cte.volume_deviation
        FROM cte
        WHERE c.ticker = cte.ticker
        ;
        
        WITH cte AS (
            SELECT
                ticker,
                SUM(CASE WHEN ABS(dividends_paid_ttm) > 0 THEN 1 ELSE 0 END) AS number_of_dividends,
                regr_r2(ABS(dividends_paid_ttm), x) AS dividend_r2,
                regr_slope(ABS(dividends_paid_ttm), x) AS dividend_slope
            FROM (
                SELECT
                    ticker,
                    dividends_paid_ttm,
                    SUM(1) OVER (PARTITION BY ticker ORDER BY time) AS x
                FROM companies_quarterly
                WHERE time > CURRENT_DATE - INTERVAL '7 years'
            ) AS tmp
            GROUP BY ticker
        )
        UPDATE companies_display c 
        SET
            dividend_consistency = CASE WHEN number_of_dividends >= 20 AND dividend_slope > 0 THEN dividend_r2 ELSE NULL END
        FROM cte
        WHERE cte.ticker = c.ticker;

    '''

    db.execute(sql)
    db.commit()

    end = time()

    print(f'Finished updating companies_display. ({end - start:.2f}s)')
