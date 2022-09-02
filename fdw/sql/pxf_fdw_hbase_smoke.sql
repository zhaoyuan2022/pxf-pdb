-- Create Hbase tables hbase_table and pxflookup
\!{{ HBASE_CMD }} shell {{ SCRIPT create_pxflookup.rb }} >/dev/null 2>&1
\!{{ HBASE_CMD }} shell {{ SCRIPT gen_small_data.rb }}

-- FDW test
CREATE SERVER h_base_smoke_test_server
	FOREIGN DATA WRAPPER hbase_pxf_fdw
	OPTIONS (config '{{ SERVER_CONFIG }}');
CREATE USER MAPPING FOR CURRENT_USER
	SERVER h_base_smoke_test_server;
CREATE FOREIGN TABLE h_base_smoke_test_foreign_table  (
		name text,
		num int,
		dub double precision,
		longnum bigint,
		bool boolean
	)
	SERVER h_base_smoke_test_server OPTIONS (resource 'hbase_small_data_table_{{ FULL_TESTNAME }}');

SELECT * FROM h_base_smoke_test_foreign_table ORDER BY name;
SELECT name, num FROM h_base_smoke_test_foreign_table WHERE num > 50 ORDER BY name;

-- clean up HBase
{{ CLEAN_UP }}\!{{ HBASE_CMD }} shell {{ SCRIPT drop_small_data.rb }} >/dev/null 2>&1
{{ CLEAN_UP }}\!rm -rf {{ SCRIPT drop_small_data.rb }} {{ SCRIPT gen_small_data.rb }} {{ SCRIPT create_pxflookup.rb }}
