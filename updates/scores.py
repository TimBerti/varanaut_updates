from time import time


def update_scores(db):

    print(f'Started scores update...')

    start = time()

    sql = '''
        UPDATE companies_annual SET
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
            altman_z_score = (
                1.2 * net_working_capital / NULLIF(total_assets, 0) + 
                1.4 * retained_earnings / NULLIF(total_assets, 0) + 
                3.3 * ebit / NULLIF(total_assets, 0) + 
                0.6 * market_cap / NULLIF(total_liabilities, 0) + 
                1 * total_revenue / NULLIF(total_assets, 0)
            )
        ;
        UPDATE companies_quarterly SET
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
            altman_z_score = (
                1.2 * net_working_capital / NULLIF(total_assets, 0) + 
                1.4 * retained_earnings_ttm / NULLIF(total_assets, 0) + 
                3.3 * ebit_ttm / NULLIF(total_assets, 0) + 
                0.6 * market_cap / NULLIF(total_liabilities, 0) + 
                1 * total_revenue_ttm / NULLIF(total_assets, 0)
            )
        ;
        
        WITH cte2 AS (
            WITH cte AS (
                SELECT 
                    ticker, 
                    time,
                    - 4.840 + 
                    0.920 * (net_receivables / NULLIF(total_revenue, 0)) /  NULLIF(LAG(net_receivables / NULLIF(total_revenue, 0)) OVER w, 0) +
                    0.528 * LAG(gross_profit_margin) OVER w / NULLIF(gross_profit_margin, 0) +
                    0.404 * (1-(total_current_assets + property_plant_equipment + long_term_investments) / NULLIF(total_assets, 0)) / NULLIF(LAG(1-(total_current_assets + property_plant_equipment + long_term_investments) / NULLIF(total_assets, 0)) OVER w, 0) +
                    0.892 * total_revenue / NULLIF(LAG(total_revenue) OVER w, 0) +
                    0.115 * LAG(depreciation / NULLIF(property_plant_equipment + depreciation,0)) OVER w / NULLIF(depreciation / NULLIF(property_plant_equipment + depreciation, 0), 0) +
                    (-0.172) * (selling_general_administrative / NULLIF(total_revenue, 0)) / NULLIF(LAG(selling_general_administrative / NULLIF(total_revenue, 0)) OVER w, 0) +
                    (-0.327) * ((total_current_liabilities + long_term_debt_total) / NULLIF(total_assets, 0)) / NULLIF(LAG((total_current_liabilities + long_term_debt_total) / NULLIF(total_assets, 0)) OVER w, 0) +
                    4.697 * (net_income_from_continuing_operations - total_cash_from_operating_activities) / NULLIF(total_assets, 0) AS beneish_m_score
                FROM companies_annual
                WINDOW w AS (PARTITION BY ticker ORDER BY time)
                ORDER BY time
            )
            SELECT DISTINCT ON (ticker) ticker, time, beneish_m_score FROM cte ORDER BY ticker, time DESC
        )
        UPDATE companies_display c
        SET beneish_m_score = cte2.beneish_m_score
        FROM cte2
        WHERE cte2.ticker = c.ticker
        ;
    '''

    db.execute(sql)
    db.commit()

    end = time()

    print(f'Finished scores update. ({end - start:.2f}s)')
