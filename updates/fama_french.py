import pandas as pd
import numpy as np
from tqdm import tqdm
from time import time


def update_fama_french_factors(db):
    '''
    Calculates the quarterly factors of the fama and french five factors model for the last 10 years.
    '''

    start = time()

    print('Started fama and french 5 factors update...')

    sql = f'''
        SELECT
            ticker,
            DATE_TRUNC('quarter', time) AS quarter,
            100 * (market_cap / NULLIF(LAG(market_cap, 4) OVER (
                PARTITION BY ticker
                ORDER BY time
            ), 0) - 1) AS return,
            market_cap,
            price_book,
            - total_cashflows_from_investing_activities_ttm / NULLIF(total_assets, 0) AS investing,
            gross_profit_ttm / NULLIF(total_assets - total_liabilities, 0) AS profitability
        FROM companies_quarterly WHERE ticker in (
            SELECT UNNEST(holdings) FROM etf WHERE ticker = 'VTI'
        )
        AND market_cap IS NOT NULL
        AND price_book IS NOT NULL
        AND total_cashflows_from_investing_activities_ttm IS NOT NULL
        AND total_assets IS NOT NULL
        AND total_liabilities IS NOT NULL
        AND gross_profit_ttm IS NOT NULL
        AND total_assets > total_liabilities
        AND time > CURRENT_DATE - INTERVAl '10 years'
        ORDER BY time DESC
        ;
    '''

    stocks_df = pd.read_sql(sql, con=db.get_bind()).dropna()

    # Market Return (SPY)

    sql = '''
        WITH RECURSIVE
            risk_free AS (
                SELECT DATE_TRUNC('quarter', time) AS quarter, AVG(adjusted_close) AS risk_free_rate
                FROM eod WHERE ticker = 'US10Y.GBOND'
                GROUP BY DATE_TRUNC('quarter', time)
                ORDER BY quarter DESC
            ),
            market AS (
                SELECT DATE_TRUNC('quarter', time) AS quarter, AVG(adjusted_close) AS average_price
                FROM eod WHERE ticker = 'SPY'
                GROUP BY DATE_TRUNC('quarter', time)
                ORDER BY quarter DESC
            )
        SELECT
            m.quarter as time,
            risk_free_rate,
            (average_price / NULLIF(LAG(average_price, 4) OVER (ORDER BY m.quarter), 0) - 1) * 100 - risk_free_rate AS excess_market_return
        FROM risk_free r
        INNER JOIN market m
        ON r.quarter = m.quarter
        ;
    '''

    market_df = pd.read_sql(sql, con=db.get_bind())

    # split in months

    quarter_groups = stocks_df.groupby(by='quarter')

    # calculate factors

    factors = []

    for quarter in quarter_groups:

        timestamp = quarter[0]

        quarter_df = quarter[1]

        quantiles = quarter_df.quantile(0.5)

        # Small Minus Big (market_cap)

        SMB = quarter_df[quarter_df['market_cap'] < quantiles['market_cap']]['return'].mean() \
            - quarter_df[quarter_df['market_cap'] >
                         quantiles['market_cap']]['return'].mean()

        # High Minus Low valuation (price_book)

        HML = quarter_df[quarter_df['price_book'] > quantiles['price_book']]['return'].mean() \
            - quarter_df[quarter_df['price_book'] <
                         quantiles['price_book']]['return'].mean()

        # Conservative Minus Aggresive Investing (total_cashflows_from_investing_activities on total_assets)

        CMA = quarter_df[quarter_df['investing'] < quantiles['investing']]['return'].mean() \
            - quarter_df[quarter_df['investing'] >
                         quantiles['investing']]['return'].mean()

        # Robust Minus Weak Profitability (gross_proft on equity)

        RMW = quarter_df[quarter_df['profitability'] > quantiles['profitability']]['return'].mean() \
            - quarter_df[quarter_df['profitability'] <
                         quantiles['profitability']]['return'].mean()

        factors.append([timestamp, SMB, HML, CMA, RMW])

    df = pd.DataFrame(factors, columns=['time', 'SMB', 'HML', 'CMA', 'RMW'])
    df = df.merge(market_df, on='time').dropna()
    df.to_sql('fama_french_factors', con=db.get_bind(),
              if_exists='replace', index=False)

    end = time()

    print(f'Finished fama and french 5 factors update. ({end - start :.2f}s)')


def calculate_fama_french_regressions(db, fama_french_df, ticker):
    '''
    Calculate fama and french regression coefficients for ticker
    '''

    sql = f'''
        WITH cte AS (
            SELECT DATE_TRUNC('quarter', time) AS quarter, AVG(adjusted_close) AS average_price
            FROM eod WHERE ticker = '{ticker}' 
            GROUP BY DATE_TRUNC('quarter', time)
            ORDER BY quarter DESC
        )
        SELECT 
            quarter AS time,
            (average_price / NULLIF(LAG(average_price, 4) OVER (ORDER BY quarter), 0) - 1) * 100 AS return
        FROM cte
        ;
    '''

    stock_df = pd.read_sql(sql, con=db.get_bind())

    stock_df = stock_df.merge(fama_french_df, on='time').dropna()

    A = stock_df[['SMB', 'HML', 'CMA', 'RMW',
                  'excess_market_return']].to_numpy()
    y = (stock_df['return'] - stock_df['risk_free_rate']).to_numpy()

    x, *_ = np.linalg.lstsq(A[:-1], y[:-1], rcond=None)

    return x


def update_fama_french_regressions(db):
    '''
    Updates fama and french regressions in db.
    '''

    sql = '''
        SELECT DISTINCT ticker FROM companies_display;
    '''

    tickers = pd.read_sql(sql, con=db.get_bind())['ticker']

    workload = len(tickers)

    print(f'Started fama and french regressions update. Workload: {workload}')

    sql = '''
        SELECT * FROM public."fama_french_factors";
    '''

    fama_french_df = pd.read_sql(sql, con=db.get_bind())

    for ticker in tqdm(tickers):

        try:

            SMB_factor, HML_factor, CMA_factor, RMW_factor, excess_market_return_factor = calculate_fama_french_regressions(
                db, fama_french_df, ticker)

            sql = f'''
                UPDATE companies_display
                SET 
                    SMB_factor = :SMB_factor,
                    HML_factor = :HML_factor,
                    CMA_factor = :CMA_factor,
                    RMW_factor = :RMW_factor,
                    excess_market_return_factor = :excess_market_return_factor
                WHERE ticker = :ticker
                ;
            '''

            db.execute(sql, {'ticker': ticker, 'SMB_factor': SMB_factor, 'HML_factor': HML_factor, 'CMA_factor': CMA_factor,
                       'RMW_factor': RMW_factor, 'excess_market_return_factor': excess_market_return_factor})
            db.commit()

        except:
            continue

    print('Finished fama and french regressions update.')
