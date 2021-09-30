CREATE TABLE indices (
    ticker VARCHAR(32),
    currency VARCHAR(32),
    name VARCHAR(255),
    country VARCHAR(255),
    components VARCHAR(32) [],
    PRIMARY KEY (ticker)
)