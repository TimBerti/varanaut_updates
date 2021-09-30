CREATE TABLE eod (
	ticker VARCHAR(32),
	time TIMESTAMP,
	open FLOAT8,
	high FLOAT8,
	low FLOAT8,
	close FLOAT8,
	adjusted_close FLOAT8,
	volume FLOAT8,
	contained_in_s_and_p500 boolean,
	PRIMARY KEY (ticker, time)
) PARTITION BY LIST (ticker);