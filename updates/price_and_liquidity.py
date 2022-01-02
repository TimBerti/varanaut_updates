from time import time


def update_price_and_liqudity(db):

    print(f'Started price and liqudity update...')

    start = time()

    sql = '''
        CREATE OR REPLACE FUNCTION get_price (VARCHAR, INT)
        RETURNS float8 AS $price$
        DECLARE
            price float8;
        BEGIN
            SELECT adjusted_close INTO price
            FROM eod WHERE ticker = $1
            ORDER BY time DESC
            LIMIT 1 OFFSET $2;
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
            stock_price = get_price(ticker, 0),
            daily_return = (get_price(ticker, 0) / NULLIF(get_price(ticker, 1), 0) - 1) * 100,
            weekly_return = (get_price(ticker, 0) / NULLIF(get_price(ticker, 5), 0) - 1) * 100,
            monthly_return = (get_price(ticker, 0) / NULLIF(get_price(ticker, 20), 0) - 1) * 100,
            quarterly_return = (get_price(ticker, 0) / NULLIF(get_price(ticker, 60), 0) - 1) * 100,
            semi_annual_return = (get_price(ticker, 0) / NULLIF(get_price(ticker, 120), 0) - 1) * 100,
            annual_return = (get_price(ticker, 0) / NULLIF(get_price(ticker, 240), 0) - 1) * 100,
            two_year_return = (get_price(ticker, 0) / NULLIF(get_price(ticker, 480), 0) - 1) * 100,
            three_year_return = (get_price(ticker, 0) / NULLIF(get_price(ticker, 720), 0) - 1) * 100,
            average_volume = get_average_volume(ticker)
        ;
    '''

    db.execute(sql)
    db.commit()

    end = time()

    print(f'Finished price and liqudity update. ({end - start:.2f}s)')
