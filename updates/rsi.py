from time import time


def update_rsi_180(db):
    '''
    Updates the 180 days RSI
    '''

    start = time()

    print('Started RSI 180 update...')

    sql = '''
        CREATE OR REPLACE FUNCTION rsi (VARCHAR, INT)
        RETURNS float8 AS $rsi$
        DECLARE
            rsi float8;
        BEGIN
            WITH split_returns AS (
                WITH returns AS (
                    SELECT 
                        time,
                        adjusted_close / NULLIF(
                            LAG(adjusted_close) OVER (
                                PARTITION BY ticker
                                ORDER BY time
                            ), 0
                        ) - 1 AS return
                    FROM eod WHERE ticker = $1
                    ORDER BY time DESC
                    LIMIT $2
                ) 
                SELECT 
                    CASE WHEN return > 0 THEN return ELSE 0 END AS positive_return, 
                    CASE WHEN return < 0 THEN - return ELSE 0 END AS negative_return
                FROM returns
            )
            SELECT 
                100 - 100 / (1 + AVG(positive_return) / NULLIF(AVG(negative_return), 0)) INTO rsi
            FROM split_returns
            ;
            RETURN rsi;
        END;
        $rsi$ LANGUAGE plpgsql;

        UPDATE companies_display SET rsi_180 = rsi(ticker, 180);
    '''

    db.execute(sql)
    db.commit()

    end = time()

    print(f'Finished RSI 180 update. ({end - start:.2f}s)')
