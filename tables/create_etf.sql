CREATE TABLE etf (
    ticker VARCHAR(32),
    currency VARCHAR(32),
    name VARCHAR(255),
    country VARCHAR(255),
    holdings VARCHAR(32) [],
    PRIMARY KEY (ticker)
)