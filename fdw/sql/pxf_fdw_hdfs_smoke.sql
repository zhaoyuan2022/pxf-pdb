CREATE SERVER hdfs_smoke_test_hdfs_server
	FOREIGN DATA WRAPPER hdfs_pxf_fdw
	OPTIONS (config 'default');
CREATE USER MAPPING FOR CURRENT_USER SERVER hdfs_smoke_test_hdfs_server;
CREATE FOREIGN TABLE hdfs_smoke_test_foreign_table (
		name TEXT,
		num INTEGER,
		dub DOUBLE PRECISION,
		longNum BIGINT,
		bool BOOLEAN
	) SERVER hdfs_smoke_test_hdfs_server
	OPTIONS (resource '/tmp/data.csv', format 'csv');
SELECT * FROM hdfs_smoke_test_foreign_table ORDER BY name;
SELECT name, num FROM hdfs_smoke_test_foreign_table WHERE num > 50 ORDER BY name;

