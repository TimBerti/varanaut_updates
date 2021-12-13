import pandas as pd
from sklearn.linear_model import LinearRegression
from time import time
from tqdm import tqdm


def update_risk_factors(db):
    '''
    Updates risk factors in companies_display.
    '''

    start = time()

    print('Started risk factors update...')

    sql = '''
        WITH RECURSIVE
            equity AS (
                SELECT	DISTINCT ON (DATE_TRUNC('month', time))
                    (DATE_TRUNC('month', time)) AS time, 
                    adjusted_close
                FROM eod WHERE ticker = 'SPY'
            ),
            interest_rate AS (
                SELECT	DISTINCT ON (DATE_TRUNC('month', time))
                    (DATE_TRUNC('month', time)) AS time, 
                    adjusted_close
                FROM eod WHERE ticker = 'SHY'
            ),
            credit AS (
                SELECT	DISTINCT ON (DATE_TRUNC('month', time))
                    (DATE_TRUNC('month', time)) AS time, 
                    adjusted_close
                FROM eod WHERE ticker = 'VCSH'
            ),
            commodities AS (
                SELECT	DISTINCT ON (DATE_TRUNC('month', time))
                    (DATE_TRUNC('month', time)) AS time, 
                    adjusted_close
                FROM eod WHERE ticker = 'GSG'
            ),
            cpi AS (
                SELECT	DISTINCT ON (DATE_TRUNC('month', time))
                    (DATE_TRUNC('month', time)) AS time, 
                    adjusted_close
                FROM eod WHERE ticker = 'CPI_US'
            )
        SELECT
            equity.time AS time,
            equity.adjusted_close AS equity,
            interest_rate.adjusted_close AS interest_rate,
            credit.adjusted_close AS credit,
            commodities.adjusted_close AS commodities,
            cpi.adjusted_close AS cpi
        FROM equity
        INNER JOIN interest_rate
        ON equity.time = interest_rate.time
        INNER JOIN credit
        ON equity.time = credit.time
        INNER JOIN commodities
        ON equity.time = commodities.time
        INNER JOIN cpi
        ON equity.time = cpi.time
        ;
    '''

    df = pd.read_sql(sql, con=db.get_bind())

    df.set_index('time', inplace=True)

    df[['equity', 'interest_rate', 'credit', 'commodities', 'cpi']] = df[['equity', 'interest_rate', 'credit',
                                                                          'commodities', 'cpi']] / df[['equity', 'interest_rate', 'credit', 'commodities', 'cpi']].shift()

    df[['equity', 'interest_rate', 'credit', 'commodities', 'cpi']] = (df[['equity', 'interest_rate', 'credit', 'commodities', 'cpi']] - df[[
                                                                       'equity', 'interest_rate', 'credit', 'commodities', 'cpi']].mean()) / df[['equity', 'interest_rate', 'credit', 'commodities', 'cpi']].std()

    sql = '''
        SELECT DISTINCT ticker FROM companies_display;
    '''

    tickers = pd.read_sql(sql, con=db.get_bind())['ticker']

    for ticker in tqdm(tickers):

        try:

            sql = f'''
                SELECT	DISTINCT ON (DATE_TRUNC('month', time))
                    (DATE_TRUNC('month', time)) AS time, 
                    adjusted_close
                FROM eod WHERE ticker = '{ticker}'
            '''

            stock_df = pd.read_sql(sql, con=db.get_bind())

            stock_df.set_index('time', inplace=True)

            stock_df['return'] = stock_df['adjusted_close'] / \
                stock_df['adjusted_close'].shift()

            stock_df.drop('adjusted_close', axis=1, inplace=True)

            df_tmp = df.join(stock_df, on='time', how='inner').dropna()

            X = df_tmp[['equity', 'interest_rate',
                        'credit', 'commodities', 'cpi']]
            y = df_tmp['return']

            reg = LinearRegression().fit(X, y)

            coefs = reg.coef_

            R2 = 1 - ((y - reg.predict(X))**2).sum() / \
                ((y - y.mean())**2).sum()

            risk_exposure = pd.DataFrame({'risk': ['intrinsic', 'equity', 'interest_rate', 'credit', 'commodities', 'inflation'], 'exposure':  [
                                         1 - R2, R2*coefs[0]/coefs.sum(), R2*coefs[1]/coefs.sum(), R2*coefs[2]/coefs.sum(), R2*coefs[3]/coefs.sum(), R2*coefs[4]/coefs.sum()]})

            sql = f'''
                UPDATE companies_display 
                SET
                    intrinsic_risk = {risk_exposure['exposure'][0]},
                    equity_risk = {risk_exposure['exposure'][1]},
                    interest_rate_risk = {risk_exposure['exposure'][2]},
                    credit_risk = {risk_exposure['exposure'][3]},
                    commodities_risk = {risk_exposure['exposure'][4]},
                    inflation_risk = {risk_exposure['exposure'][5]}
                WHERE ticker = '{ticker}'
            '''

            db.execute(sql)
            db.commit()

        except:
            pass

    end = time()

    print(f'Finished risk factor update. ({end-start:.2f}s)')
