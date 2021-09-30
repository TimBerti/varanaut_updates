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
            )
        ;
    '''

    db.execute(sql)
    db.commit()

    end = time()

    print(f'Finished scores update. ({end - start:.2f}s)')
