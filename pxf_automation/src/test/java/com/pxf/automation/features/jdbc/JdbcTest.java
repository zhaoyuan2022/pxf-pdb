package com.pxf.automation.features.jdbc;

import java.io.File;

import org.testng.annotations.Test;

import com.pxf.automation.enums.EnumPartitionType;

import com.pxf.automation.structures.tables.basic.Table;
import com.pxf.automation.structures.tables.pxf.ExternalTable;
import com.pxf.automation.structures.tables.utils.TableFactory;
import com.pxf.automation.features.BaseFeature;

public class JdbcTest extends BaseFeature {

    private static final String POSTGRES_DRIVER_CLASS = "org.postgresql.Driver";
    private static final String HAWQ_PXF_AUTOMATION_DB_JDBC = "jdbc:postgresql:pxfautomation//";
    private static final String[] TYPES_TABLE_FIELDS = new String[] {
        "t1    text",
        "t2    text",
        "num1  int",
        "dub1  double precision",
        "dec1  numeric",
        "tm timestamp",
        "r real",
        "bg bigint",
        "b boolean",
        "tn smallint",
        "sml smallint",
        "dt date",
        "vc1 varchar(5)",
        "c1 char(3)",
        "bin bytea" };
    private ExternalTable pxfJdbcSingleFragment;
    private ExternalTable pxfJdbcMultipleFragmentsByInt;
    private ExternalTable pxfJdbcMultipleFragmentsByDate;
    private ExternalTable pxfJdbcMultipleFragmentsByEnum;

    final String hawqTypesDataFileName = "hawq_types.txt";
    private Table hawqNativeTable;

    @Override
    protected void beforeClass() throws Exception {
        prepareData();
    }

    protected void prepareData() throws Exception {
        prepareTypesData();
        prepareSingleFragment();
        prepareMultipleFragmentsByInt();
        prepareMultipleFragmentsByDate();
        prepareMultipleFragmentsByEnum();
    }

    private void prepareTypesData() throws Exception {
        hawqNativeTable = new Table("hawq_types",
                TYPES_TABLE_FIELDS);
        hawqNativeTable.setDistributionFields(new String[] { "t1" });
        hawq.createTableAndVerify(hawqNativeTable);
        hawq.copyFromFile(hawqNativeTable, new File(localDataResourcesFolder
                + "/hawq/" + hawqTypesDataFileName), "\\t", "\\\\N", true);
    }

    private void prepareSingleFragment() throws Exception {
        pxfJdbcSingleFragment = TableFactory.getPxfJdbcTable(
                "pxf_jdbc_single_fragment",
                TYPES_TABLE_FIELDS,
                hawqNativeTable.getName(), 
                POSTGRES_DRIVER_CLASS,
                HAWQ_PXF_AUTOMATION_DB_JDBC + hawq.getHost() + ":" + hawq.getPort(),
                hawq.getUserName());
        pxfJdbcSingleFragment.setHost(pxfHost);
        pxfJdbcSingleFragment.setPort(pxfPort);
        hawq.createTableAndVerify(pxfJdbcSingleFragment);
    }

    private void prepareMultipleFragmentsByEnum() throws Exception {
        pxfJdbcMultipleFragmentsByEnum = TableFactory
                .getPxfJdbcPartitionedTable(
                "pxf_jdbc_multiple_fragments_by_enum", 
                TYPES_TABLE_FIELDS,
                hawqNativeTable.getName(),
                POSTGRES_DRIVER_CLASS,
                HAWQ_PXF_AUTOMATION_DB_JDBC + hawq.getHost() + ":" + hawq.getPort(),
                13,
                "USD:UAH",
                "1",
                hawq.getUserName(),
                EnumPartitionType.ENUM);
        pxfJdbcSingleFragment.setHost(pxfHost);
        pxfJdbcSingleFragment.setPort(pxfPort);
        hawq.createTableAndVerify(pxfJdbcMultipleFragmentsByEnum);
    }

    private void prepareMultipleFragmentsByInt() throws Exception {
        pxfJdbcMultipleFragmentsByInt = TableFactory
                .getPxfJdbcPartitionedTable(
                "pxf_jdbc_multiple_fragments_by_int",
                TYPES_TABLE_FIELDS,
                hawqNativeTable.getName(),
                POSTGRES_DRIVER_CLASS,
                HAWQ_PXF_AUTOMATION_DB_JDBC,
                2,
                "1:6",
                "1",
                hawq.getUserName(),
                EnumPartitionType.INT);
        pxfJdbcSingleFragment.setHost(pxfHost);
        pxfJdbcSingleFragment.setPort(pxfPort);
        hawq.createTableAndVerify(pxfJdbcMultipleFragmentsByInt);
    }

    private void prepareMultipleFragmentsByDate() throws Exception {
        pxfJdbcMultipleFragmentsByDate = TableFactory
                .getPxfJdbcPartitionedTable(
                "pxf_jdbc_multiple_fragments_by_date",
                TYPES_TABLE_FIELDS,
                hawqNativeTable.getName(),
                POSTGRES_DRIVER_CLASS,
                HAWQ_PXF_AUTOMATION_DB_JDBC + hawq.getHost() + ":" + hawq.getPort(),
                11,
                "2015-03-06:2015-03-20",
                "1:DAY",
                hawq.getUserName(),
                EnumPartitionType.DATE);
        pxfJdbcSingleFragment.setHost(pxfHost);
        pxfJdbcSingleFragment.setPort(pxfPort);
        hawq.createTableAndVerify(pxfJdbcMultipleFragmentsByDate);
    }

    @Test(groups = "features")
    public void singleFragmentTable() throws Exception {
        runTincTest("pxf.features.jdbc.single_fragment.runTest");
    }

    @Test(groups = "features")
    public void multipleFragmentsTables() throws Exception {
        runTincTest("pxf.features.jdbc.multiple_fragments.runTest");
    }
}
