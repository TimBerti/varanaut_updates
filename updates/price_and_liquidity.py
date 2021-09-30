from time import time


def update_price_and_liqudity(db):

    print(f'Started price and liqudity update...')

    start = time()

    sql = '''
        CREATE OR REPLACE FUNCTION get_price (VARCHAR)
        RETURNS float8 AS $price$
        DECLARE
            price float8;
        BEGIN
            SELECT FIRST_VALUE(adjusted_close) OVER (
                ORDER BY time DESC
            ) INTO price FROM eod 
            WHERE ticker = $1
            LIMIT 1;
            RETURN price;
        END;
        $price$ LANGUAGE plpgsql;

        CREATE OR REPLACE FUNCTION get_average_volume (VARCHAR)
        RETURNS float8 AS $average_volume$
        DECLARE
            average_volume float8;
        BEGIN
            SELECT AVG(volume) INTO average_volume FROM eod 
            WHERE ticker = $1 AND time > CURRENT_DATE - INTERVAL '3 months';
            RETURN average_volume;
        END;
        $average_volume$ LANGUAGE plpgsql;

        UPDATE companies_display SET 
        stock_price = get_price(ticker),
        average_volume = get_average_volume(ticker)
        ;
    '''

    db.execute(sql)
    db.commit()

    end = time()

    print(f'Finished price and liqudity update. ({end - start:.2f}s)')
