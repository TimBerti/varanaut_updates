CREATE TABLE companies_statistics (
	metric VARCHAR(255),
	count INT,
	mean FLOAT8,
	std FLOAT8,
	quantile_25 FLOAT8,
	quantile_50 FLOAT8,
	quantile_75 FLOAT8,
	bin_width FLOAT8,
	PRIMARY KEY (metric)
)