from time import time


def update_operating_margin(db):

    print(f'Started operating margin update...')

    start = time()

    sql = '''
        WITH cte AS (
            SELECT 
                ticker,
                MIN(operating_margin) AS min_operating_margin, 
                MAX(operating_margin) AS max_operating_margin
            FROM companies_quarterly
            WHERE time >= CURRENT_DATE - INTERVAL '5 years'
            GROUP BY ticker
        )
        UPDATE companies_display c
        SET
            operating_margin_mid = (CASE WHEN max_operating_margin < 0 THEN 0 ELSE max_operating_margin END + 
            CASE WHEN min_operating_margin < 0 THEN 0 ELSE min_operating_margin END) / 200,
            operating_margin_range = (CASE WHEN max_operating_margin < 0 THEN 0 ELSE max_operating_margin END - 
            CASE WHEN min_operating_margin < 0 THEN 0 ELSE min_operating_margin END) / 200
        FROM cte
        WHERE c.ticker = cte.ticker
        ;
    '''

    db.execute(sql)
    db.commit()

    end = time()

    print(f'Finished operating margin update. ({end - start:.2f}s)')
