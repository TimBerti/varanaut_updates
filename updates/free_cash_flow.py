from time import time


def update_free_cash_flow(db):

    print(f'Started free cash flow update...')

    start = time()

    sql = '''
        WITH descriptive_table AS (
            WITH free_cash_flow_change_table AS (
                WITH non_negative_free_cash_flows AS (
                    WITH negative_free_cash_flow_table AS (
                        SELECT 
                            ticker,
                            SUM(CASE WHEN free_cashflow <= 0 THEN 1 ELSE 0 END) AS negative_free_cash_flows
                        FROM companies_display
                        WHERE time > CURRENT_DATE - INTERVAL '6 years'
                        GROUP BY ticker
                    )
                    SELECT ticker 
                    FROM negative_free_cash_flow_table
                    WHERE negative_free_cash_flows = 0
                )
                SELECT 
                    ticker,
                    free_cashflow_ttm / NULLIF(LAG(free_cashflow_ttm, 4) OVER (
                        PARTITION BY ticker ORDER BY time
                    ), 0) AS free_cash_flow_change 
                FROM companies_quarterly 
                WHERE time <= CURRENT_DATE - INTERVAL '5 years'
                AND ticker IN (SELECT ticker FROM non_negative_free_cash_flows)
            )
            SELECT
                ticker,
                AVG(free_cash_flow_change) AS annual_free_cash_flow_change_average,
                STDDEV(free_cash_flow_change) AS annual_free_cash_flow_change_std
            FROM free_cash_flow_change_table
            GROUP BY ticker
        )
        UPDATE companies_display c
        SET 
            annual_free_cash_flow_change_average = d.annual_free_cash_flow_change_average,
            annual_free_cash_flow_change_std = d.annual_free_cash_flow_change_std
        FROM descriptive_table d
        WHERE d.ticker = c.ticker;
    '''

    db.execute(sql)
    db.commit()

    end = time()

    print(f'Finished free cash flow update. ({end - start:.2f}s)')
