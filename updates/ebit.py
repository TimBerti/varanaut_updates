from time import time


def update_ebit(db):

    print(f'Started ebit update...')

    start = time()

    sql = '''
        WITH descriptive_table AS (
            WITH ebit_change_table AS (
                WITH non_negative_ebits AS (
                    WITH negative_ebit_table AS (
                        SELECT 
                            ticker,
                            SUM(CASE WHEN ebit <= 0 THEN 1 ELSE 0 END) AS negative_ebits
                        FROM companies_display
                        WHERE time > CURRENT_DATE - INTERVAL '6 years'
                        GROUP BY ticker
                    )
                    SELECT ticker 
                    FROM negative_ebit_table
                    WHERE negative_ebits = 0
                )
                SELECT 
                    ticker,
                    ebit / NULLIF(LAG(ebit, 4) OVER (
                        PARTITION BY ticker ORDER BY time
                    ), 0) AS ebit_change 
                FROM companies_quarterly 
                WHERE time <= CURRENT_DATE - INTERVAL '5 years'
                AND ticker IN (SELECT ticker FROM non_negative_ebits)
            )
            SELECT
                ticker,
                AVG(ebit_change) AS annual_ebit_change_average,
                STDDEV(ebit_change) AS annual_ebit_change_std
            FROM ebit_change_table
            GROUP BY ticker
        )
        UPDATE companies_display c
        SET 
            annual_ebit_change_average = d.annual_ebit_change_average,
            annual_ebit_change_std = d.annual_ebit_change_std
        FROM descriptive_table d
        WHERE d.ticker = c.ticker;
    '''

    db.execute(sql)
    db.commit()

    end = time()

    print(f'Finished ebit update. ({end - start:.2f}s)')
