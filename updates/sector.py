from time import time


def update_sector(db):
    '''
    Updates sector table
    '''

    start = time()

    print('Started sector update...')

    sql = '''
        WITH cte AS (
            SELECT 
                sector, 
                COUNT(*) AS count,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ev_ebit) AS ev_ebit,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ev_ebitda) AS ev_ebitda,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_earnings) AS price_earnings,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_ebit) AS price_ebit,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_sales) AS price_sales,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_cash_flow) AS price_cash_flow,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_book) AS price_book,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY dividend_yield) AS dividend_yield,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY payout_ratio) AS payout_ratio,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY gross_profit_margin) AS gross_profit_margin,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY net_profit_margin) AS net_profit_margin,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY operating_cash_flow_margin) AS operating_cash_flow_margin,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY operating_margin) AS operating_margin,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY return_on_assets) AS return_on_assets,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY return_on_net_tangible_assets) AS return_on_net_tangible_assets,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY asset_turnover) AS asset_turnover,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY cash_flow_on_assets) AS cash_flow_on_assets,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY return_on_equity) AS return_on_equity,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY revenue_on_equity) AS revenue_on_equity,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY return_on_capital_employed) AS return_on_capital_employed,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY return_on_capital) AS return_on_capital,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY debt_ratio) AS debt_ratio,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY debt_to_ebit) AS debt_to_ebit,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY debt_to_equity) AS debt_to_equity,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY debt_to_revenue) AS debt_to_revenue,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY current_ratio) AS current_ratio,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_earnings_growth) AS price_earnings_growth,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_sales_growth) AS price_sales_growth,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY revenue_growth_1y) AS revenue_growth_1y,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY revenue_growth_3y) AS revenue_growth_3y,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY revenue_growth_5y) AS revenue_growth_5y,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY revenue_growth_9y) AS revenue_growth_9y,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY earnings_growth_1y) AS earnings_growth_1y,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY earnings_growth_3y) AS earnings_growth_3y,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY earnings_growth_5y) AS earnings_growth_5y,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY earnings_growth_9y) AS earnings_growth_9y,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY dividend_growth_1y) AS dividend_growth_1y,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY dividend_growth_3y) AS dividend_growth_3y,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY dividend_growth_5y) AS dividend_growth_5y,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY dividend_growth_9y) AS dividend_growth_9y,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY daily_return) AS daily_return,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY weekly_return) AS weekly_return,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY monthly_return) AS monthly_return,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY quarterly_return) AS quarterly_return,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY semi_annual_return) AS semi_annual_return,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY annual_return) AS annual_return,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY two_year_return) AS two_year_return,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY three_year_return) AS three_year_return
            FROM companies_display 
            GROUP BY sector
        )
        UPDATE sector s
        SET    
            count = cte.count,
            ev_ebit = cte.ev_ebit,
            ev_ebitda = cte.ev_ebitda,
            price_earnings = cte.price_earnings,
            price_ebit = cte.price_ebit,
            price_sales = cte.price_sales,
            price_cash_flow = cte.price_cash_flow,
            price_book = cte.price_book,
            dividend_yield = cte.dividend_yield,
            payout_ratio = cte.payout_ratio,
            gross_profit_margin = cte.gross_profit_margin,
            net_profit_margin = cte.net_profit_margin,
            operating_cash_flow_margin = cte.operating_cash_flow_margin,
            operating_margin = cte.operating_margin,
            return_on_assets = cte.return_on_assets,
            return_on_net_tangible_assets = cte.return_on_net_tangible_assets,
            asset_turnover = cte.asset_turnover,
            cash_flow_on_assets = cte.cash_flow_on_assets,
            return_on_equity = cte.return_on_equity,
            revenue_on_equity = cte.revenue_on_equity,
            return_on_capital_employed = cte.return_on_capital_employed,
            return_on_capital = cte.return_on_capital,
            debt_ratio = cte.debt_ratio,
            debt_to_ebit = cte.debt_to_ebit,
            debt_to_equity = cte.debt_to_equity,
            debt_to_revenue = cte.debt_to_revenue,
            current_ratio = cte.current_ratio,
            price_earnings_growth = cte.price_earnings_growth,
            price_sales_growth = cte.price_sales_growth,
            revenue_growth_1y = cte.revenue_growth_1y,
            revenue_growth_3y = cte.revenue_growth_3y,
            revenue_growth_5y = cte.revenue_growth_5y,
            revenue_growth_9y = cte.revenue_growth_9y,
            earnings_growth_1y = cte.earnings_growth_1y,
            earnings_growth_3y = cte.earnings_growth_3y,
            earnings_growth_5y = cte.earnings_growth_5y,
            earnings_growth_9y = cte.earnings_growth_9y,
            dividend_growth_1y = cte.dividend_growth_1y,
            dividend_growth_3y = cte.dividend_growth_3y,
            dividend_growth_5y = cte.dividend_growth_5y,
            dividend_growth_9y = cte.dividend_growth_9y,
            daily_return = cte.daily_return,
            weekly_return = cte.weekly_return,
            monthly_return = cte.monthly_return,
            quarterly_return = cte.quarterly_return,
            semi_annual_return = cte.semi_annual_return,
            annual_return = cte.annual_return,
            two_year_return = cte.two_year_return,
            three_year_return = cte.three_year_return
        FROM cte
        WHERE s.sector = cte.sector
        ;
    '''

    db.execute(sql)
    db.commit()

    end = time()

    print(f'Finished sector update. ({end - start:.2f}s)')
