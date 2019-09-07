t = create 'hbase_small_data_table_{{ FULL_TESTNAME }}', 'col'

(1..100).each { |i|
	row_name = "row_#{i}"
	t.put row_name, 'col:name', row_name
	t.put row_name, 'col:number', "#{i}"
	t.put row_name, 'col:doub', "#{1.001 * i}"
	t.put row_name, 'col:longnum', "#{i * 100000000000}"
	t.put row_name, 'col:bool', "#{i % 2 == 0}"
}

put 'pxflookup', 'hbase_small_data_table_{{ FULL_TESTNAME }}', 'mapping:name', 'col:name'
put 'pxflookup', 'hbase_small_data_table_{{ FULL_TESTNAME }}', 'mapping:num', 'col:number'
put 'pxflookup', 'hbase_small_data_table_{{ FULL_TESTNAME }}', 'mapping:dub', 'col:doub'
put 'pxflookup', 'hbase_small_data_table_{{ FULL_TESTNAME }}', 'mapping:longnum', 'col:longnum'
put 'pxflookup', 'hbase_small_data_table_{{ FULL_TESTNAME }}', 'mapping:bool', 'col:bool'

exit
