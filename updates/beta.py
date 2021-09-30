import pandas as pd
from tqdm import tqdm
from time import time


def get_tickers(db):

    sql = '''
    SELECT ticker FROM companies_display;
    '''

    df = pd.read_sql(sql, con=db.engine)

    return df['ticker']


def update_beta(db):

    tickers = get_tickers(db)

    workload = len(tickers)

    start = time()

    print(f'Updating beta, workload: {workload}')

    for ticker in tqdm(tickers):

        sql = f'''
        CREATE OR REPLACE FUNCTION calculate_beta (VARCHAR)
        RETURNS float8 AS $beta$
        DECLARE
            beta float8;
        BEGIN
            WITH weekly_returns AS (
                WITH RECURSIVE
                market AS (
                    SELECT time, adjusted_close FROM eod WHERE ticker = 'SPY'
                ),
                stock AS (
                    SELECT time, adjusted_close FROM eod WHERE ticker = $1
                )
                SELECT
                    market.time,
                    stock.adjusted_close / NULLIF(LAG(stock.adjusted_close) OVER (ORDER BY market.time DESC), 0) AS stock,
                    market.adjusted_close / NULLIF(LAG(market.adjusted_close) OVER (ORDER BY market.time DESC), 0) AS market
                FROM stock
                JOIN market
                ON stock.time = market.time
                WHERE EXTRACT(DOW FROM market.time) = 1
                ORDER BY market.time DESC
                LIMIT 52
            )
            SELECT 
                COVAR_SAMP(stock, market) / VAR_SAMP(market) INTO beta
            FROM weekly_returns
            ;
            RETURN beta;
        END;
        $beta$ LANGUAGE plpgsql;

        UPDATE companies_display
        SET beta = calculate_beta('{ticker}')
        WHERE ticker = '{ticker}'
        ;
        '''

        db.execute(sql)
        db.commit()

    end = time()

    print(f'Finished beta update. ({end - start:.2f}s)')
