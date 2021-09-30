CREATE TABLE forex_rates (
	ticker VARCHAR(32),
	time TIMESTAMP,
	open FLOAT8,
	high FLOAT8,
	low FLOAT8,
	close FLOAT8,
    PRIMARY KEY (ticker, time)
) PARTITION BY LIST (ticker);