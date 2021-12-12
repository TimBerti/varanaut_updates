CREATE TABLE options (
    ticker VARCHAR(32),
    expiration_date TIMESTAMP,
    implied_volatility double precision,
    put_volume double precision,
    call_volume double precision,
    put_open_interest double precision,
    call_open_interest double precision,
    options_count double precision,
    PRIMARY KEY (ticker, expiration_date)
) PARTITION BY LIST (ticker);