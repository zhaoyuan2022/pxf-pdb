-- Create Hbase tables hbase_table and pxflookup
\!{{ HBASE_CMD }} shell {{ SCRIPT create_pxflookup.rb }} >/dev/null 2>&1
\!{{ HBASE_CMD }} shell {{ SCRIPT gen_small_data.rb }}

-- External Table test
CREATE EXTERNAL TABLE h_base_smoke_test_external_table
	(name TEXT, num INTEGER, dub DOUBLE PRECISION, longnum BIGINT, bool BOOLEAN)
	LOCATION('pxf://hbase_small_data_table_{{ FULL_TESTNAME }}?PROFILE=HBase{{ SERVER_PARAM }}')
	FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

SELECT * FROM h_base_smoke_test_external_table ORDER BY name;
SELECT name, num FROM h_base_smoke_test_external_table WHERE num > 50 ORDER BY name;

-- clean up HBase
{{ CLEAN_UP }}\!{{ HBASE_CMD }} shell {{ SCRIPT drop_small_data.rb }} >/dev/null 2>&1
{{ CLEAN_UP }}\!rm -rf {{ SCRIPT drop_small_data.rb }} {{ SCRIPT gen_small_data.rb }} {{ SCRIPT create_pxflookup.rb }}
