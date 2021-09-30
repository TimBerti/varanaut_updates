from time import time


def update_forex_rate(db):

    print(f'Started forex rates update...')

    start = time()

    sql = '''
        CREATE OR REPLACE FUNCTION get_forex_rate (VARCHAR, TIMESTAMP)
        RETURNS float8 AS $forex_rate$
        DECLARE
            forex_rate float8;
        BEGIN
            SELECT FIRST_VALUE(close) OVER (
                ORDER BY time DESC
            ) INTO forex_rate FROM forex_rates 
            WHERE ticker = $1 AND time <= $2
            LIMIT 1;
            RETURN forex_rate;
        END;
        $forex_rate$ LANGUAGE plpgsql;

        UPDATE companies_annual
        SET forex_rate = get_forex_rate(currency, time)
        ;

        UPDATE companies_quarterly
        SET forex_rate = get_forex_rate(currency, time)
        ;
    '''

    db.execute(sql)
    db.commit()

    end = time()

    print(f'Finished forex rate update. ({end - start:.2f}s)')
