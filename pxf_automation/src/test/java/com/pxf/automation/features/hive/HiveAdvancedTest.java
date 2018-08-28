package com.pxf.automation.features.hive;

import annotations.ExpectedFailure;

import com.pxf.automation.components.cluster.PhdCluster.EnumClusterServices;
import com.pxf.automation.structures.tables.hive.HiveTable;
import com.pxf.automation.structures.tables.utils.TableFactory;
import com.pxf.automation.AdvancedFunctionality;

/** Tests Hive plugin in PXF */
public class HiveAdvancedTest extends AdvancedFunctionality {

    @Override
    protected void afterClass() throws Exception {
        cluster.start(EnumClusterServices.hive);
    }

    /**
     * Stop Hive metastore, run query and expect error message, and restart Hive
     * metastore.
     *
     * TODO: enable test when issue is resolved.
     *
     * @throws Exception if test fails to run
     */
    // @Test(groups = "features")
    @ExpectedFailure(reason = "[#91052648] - Can't start/stop Hive MetaStore in Ambari setup.")
    public void hiveMetastoreDown() throws Exception {
        HiveTable hiveTable = new HiveTable("hive_table", null);
        exTable = TableFactory.getPxfHiveReadableTable(
                "pxf_hive_metastore_down", new String[] {
                        "t1 text",
                        "num1 integer" }, hiveTable, true);
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);
        hawq.createTableAndVerify(exTable);
        cluster.stop(EnumClusterServices.hive);
        runTincTest("pxf.features.hive.errors.hiveMetastoreDown.runTest");
    }
}
