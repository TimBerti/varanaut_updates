from time import time


def update_statistics(db):
    '''
    Calculates the statistics of all figures in db.
    '''

    print('Calculating statistics...')

    start = time()

    sql = '''
        SELECT column_name
        FROM information_schema.columns 
        WHERE table_name = 'companies_display'
        AND data_type = 'double precision'
        ;
    '''

    columns = [x[0] for x in db.execute(sql)]

    column_statistics = [f'''
        WITH cte AS (
            SELECT
                '{column}' AS metric,
                COUNT({column}) count,
                AVG({column}) mean,
                STDDEV({column}) std,
                PERCENTILE_CONT(.25) WITHIN GROUP (
                    ORDER BY {column}
                ) quantile_25,
                PERCENTILE_CONT(.5) WITHIN GROUP (
                    ORDER BY {column}
                ) quantile_50,
                PERCENTILE_CONT(.75) WITHIN GROUP (
                    ORDER BY {column}
                ) quantile_75
            FROM companies_display
        )
        INSERT INTO companies_statistics(
            metric,
            count,
            mean,
            std,
            quantile_25,
            quantile_50,
            quantile_75,
            bin_width
        )
        SELECT
            cte.metric,
            cte.count,
            cte.mean,
            cte.std,
            cte.quantile_25,
            cte.quantile_50,
            cte.quantile_75,
            2 * (cte.quantile_75 - cte.quantile_25) / CBRT(cte.count)
        FROM cte
        ON CONFLICT (metric) DO 
            UPDATE
            SET
                count = EXCLUDED.count,
                mean = EXCLUDED.mean,
                std = EXCLUDED.std,
                quantile_25 = EXCLUDED.quantile_25,
                quantile_50 = EXCLUDED.quantile_50,
                quantile_75 = EXCLUDED.quantile_75,
                bin_width = 2 * (EXCLUDED.quantile_75 - EXCLUDED.quantile_25) / CBRT(EXCLUDED.count)
        ;
    ''' for column in columns]

    sql = ''' '''.join(column_statistics)

    db.execute(sql)
    db.commit()

    end = time()

    print(f'Finished calculating statistics. ({end - start:.2f}s)')
