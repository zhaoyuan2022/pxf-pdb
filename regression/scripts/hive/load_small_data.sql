CREATE TABLE hive_small_data_table_{{ FULL_TESTNAME }} (
		s1 string,
		n1 int,
		d1 double,
		bg bigint,
		b boolean
	)
	ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
	STORED AS textfile;
LOAD DATA LOCAL INPATH '{{ TEST_LOCATION }}/data.csv'
	INTO TABLE  hive_small_data_table_{{ FULL_TESTNAME }};
