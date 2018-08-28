-- @description query01 for PXF Hive filter pushdown case
SET optimizer = off;
SET gp_external_enable_filter_pushdown = on;

SELECT * FROM test_filter WHERE  t0 = 'A' and a1 = 0 ORDER BY t0, a1;

SELECT * FROM test_filter WHERE  t0 = 'B' AND a1 <= 1 ORDER BY t0, a1
