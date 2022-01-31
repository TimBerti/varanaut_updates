from time import time


def update_revenue(db):

    print(f'Started revenue update...')

    start = time()

    sql = '''
        WITH descriptive_table AS (
            WITH revenue_change_table AS (
                SELECT 
                    ticker,
                    total_revenue_ttm / NULLIF(LAG(total_revenue_ttm, 4) OVER (
                        PARTITION BY ticker ORDER BY time
                    ), 0) AS revenue_change 
                FROM companies_quarterly 
                WHERE time >= CURRENT_DATE - INTERVAL '5 years'
            )
            SELECT
                ticker,
                AVG(revenue_change) AS annual_revenue_change_average,
                STDDEV(revenue_change) AS annual_revenue_change_std
            FROM revenue_change_table
            GROUP BY ticker
        )
        UPDATE companies_display c
        SET 
            annual_revenue_change_average = d.annual_revenue_change_average,
            annual_revenue_change_std = d.annual_revenue_change_std
        FROM descriptive_table d
        WHERE d.ticker = c.ticker;
    '''

    db.execute(sql)
    db.commit()

    end = time()

    print(f'Finished revenue update. ({end - start:.2f}s)')
