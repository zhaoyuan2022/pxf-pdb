CREATE TABLE hive_small_data_table_{{ FULL_TESTNAME }} (
		name string,
		num int,
		dub double,
		longNum bigint,
		bool boolean
	)
	ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
	STORED AS textfile;
LOAD DATA LOCAL INPATH '{{ TEST_LOCATION }}/data.csv'
	INTO TABLE  hive_small_data_table_{{ FULL_TESTNAME }};
