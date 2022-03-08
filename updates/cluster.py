import pandas as pd
import numpy as np
from tqdm import tqdm
from time import time
from sklearn.cluster import KMeans


def sigmoid_scaler(x):
    return 2 / (1 + np.exp(-2 * x)) - 1


def update_clusters(db):
    '''
    Assigns a cluster to each stock based on its Fama and French factors and updates DB.
    '''

    start = time()

    print('Started cluster update...')

    sql = '''
        SELECT 
            ticker,
            SMB_factor, 
            HML_factor, 
            CMA_factor, 
            RMW_factor 
        from companies_display;
    '''

    df = pd.read_sql(sql, con=db.get_bind())

    df['norm'] = np.sqrt(
        (df[['smb_factor', 'hml_factor', 'cma_factor', 'rmw_factor']]**2).sum(axis=1))

    df_normed = pd.DataFrame()
    df_normed['ticker'] = df['ticker']
    df_normed['smb_factor'] = df['smb_factor'] / df['norm']
    df_normed['hml_factor'] = df['hml_factor'] / df['norm']
    df_normed['cma_factor'] = df['cma_factor'] / df['norm']
    df_normed['rmw_factor'] = df['rmw_factor'] / df['norm']

    df_normed.dropna(inplace=True)

    kmeans = KMeans(n_clusters=6)
    df_normed['cluster'] = kmeans.fit_predict(
        df_normed[['smb_factor', 'hml_factor', 'cma_factor', 'rmw_factor']])

    print(f'Workload: {len(df_normed.index)}')

    for _, row in tqdm(df_normed.iterrows()):
        sql = f'''
            UPDATE companies_display
            SET cluster = {row['cluster']}
            WHERE ticker = '{row['ticker']}'
            ;
        '''

        db.execute(sql)
        db.commit()

    end = time()

    print(f'Finished cluster update. ({end - start :.2f}s)')


def update_cluster_correlation(db):
    '''
        Calculates correlation between the clusters and updates DB.
    '''

    start = time()

    print('Started cluster correlation update...')

    # data

    sql = '''
        SELECT 
            ticker, 
            time, 
            adjusted_close,
            adjusted_close / NULLIF(
                LAG(adjusted_close) OVER (
                    PARTITION BY ticker
                    ORDER BY time
                ), 0
            ) AS return
        FROM eod
        WHERE time > CURRENT_DATE - INTERVAL '5 YEARS'
    '''

    prices = pd.read_sql(sql, con=db.get_bind())

    sql = '''
        SELECT
            ticker,
            cluster
        FROM companies_display WHERE cluster IS NOT NULL
    '''

    clusters = pd.read_sql(sql, con=db.get_bind())

    sql = '''
        SELECT
            ticker,
            time,
            outstanding_shares
        FROM companies_quarterly 
        WHERE outstanding_shares IS NOT NULL
        AND time > CURRENT_DATE - INTERVAL '5 YEARS'
    '''

    shares = pd.read_sql(sql, con=db.get_bind())

    # merge dfs

    df = prices.merge(clusters, on='ticker', how='inner')

    df['quarter'] = pd.DatetimeIndex(df['time']).to_period('Q')
    shares['quarter'] = pd.DatetimeIndex(shares['time']).to_period('Q')
    shares.drop('time', axis=1, inplace=True)

    df = df.merge(shares, on=['ticker', 'quarter'], how='outer')

    df['market_cap'] = df['adjusted_close'] * df['outstanding_shares']

    df.dropna(inplace=True)

    # calculate daily returns for each cluster

    clusters = {}

    for idx, cluster in df.groupby('cluster'):

        clusters[idx] = cluster.groupby('time').apply(
            lambda x: np.average(x['return'], weights=x['market_cap']))

    cluster_df = pd.DataFrame(clusters)

    # update db

    cluster_df.corr().to_sql('cluster_correlation', con=db.get_bind(),
                             index=False, if_exists='replace')

    end = time()

    print(f'Finished cluster correlation update. ({end - start :.2f}s)')
