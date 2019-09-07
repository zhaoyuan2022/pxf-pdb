!connect 'jdbc:hive2://{{ HIVE_HOST }}:10000/default{{ HIVE_PRINCIPAL }}' "" "" ""
DROP DATABASE hive_smoke_test_database_{{ FULL_TESTNAME }} CASCADE;
