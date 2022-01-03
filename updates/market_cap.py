from time import time


def update_market_cap(db):

    print(f'Started market cap update...')

    start = time()

    sql = '''
        CREATE OR REPLACE FUNCTION get_price_at_report (VARCHAR, TIMESTAMP)
        RETURNS float8 AS $price_at_report$
        DECLARE
            price_at_report float8;
        BEGIN
            SELECT FIRST_VALUE(adjusted_close) OVER (
                ORDER BY time DESC
            ) INTO price_at_report FROM eod 
            WHERE ticker = $1 AND time BETWEEN $2 - INTERVAL '7 days' AND $2
            LIMIT 1;
            RETURN price_at_report;
        END;
        $price_at_report$ LANGUAGE plpgsql;

        UPDATE companies_annual
        SET market_cap = outstanding_shares * get_price_at_report(ticker, time)
        ;

        UPDATE companies_quarterly
        SET market_cap = outstanding_shares * get_price_at_report(ticker, time)
        ;
    '''

    db.execute(sql)
    db.commit()

    end = time()

    print(f'Finished market cap update. ({end - start:.2f}s)')
