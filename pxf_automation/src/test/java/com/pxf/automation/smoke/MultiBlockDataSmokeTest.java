package com.pxf.automation.smoke;

import org.testng.annotations.Test;

import com.pxf.automation.structures.tables.basic.Table;
import com.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import com.pxf.automation.utils.fileformats.FileFormatsUtils;
import com.pxf.automation.datapreparer.MultiLineSmokeDataPreparer;

/** PXF on large HDFS text file (268.13 MB) */
public class MultiBlockDataSmokeTest extends BaseSmoke {
    protected String multiBlockedFile = "multiblock.csv";

    @Override
    protected void prepareData() throws Exception {
        // Create local Large file and copy to HDFS
        String textFilePath = hdfs.getWorkingDirectory() + "/" + multiBlockedFile;
        String localDataFile = dataTempFolder + "/" + multiBlockedFile;

        Table dataTable = new Table("dataTable", null);

        FileFormatsUtils.prepareData(new MultiLineSmokeDataPreparer(), 1000, dataTable);
        FileFormatsUtils.prepareDataFile(dataTable, 32000, localDataFile);

        hdfs.copyFromLocal(localDataFile, textFilePath);
    }

    @Override
    protected void createTables() throws Exception {
        // Create HAWQ external table
        exTable = new ReadableExternalTable("pxf_smoke_multi_blocked_data",
                new String[] { "t1 text", "a1 integer" }, hdfs.getWorkingDirectory() + "/" + multiBlockedFile, "TEXT");
        exTable.setFragmenter("org.apache.hawq.pxf.plugins.hdfs.HdfsDataFragmenter");
        exTable.setAccessor("org.apache.hawq.pxf.plugins.hdfs.LineBreakAccessor");
        exTable.setResolver("org.apache.hawq.pxf.plugins.hdfs.StringPassResolver");
        exTable.setDelimiter(",");
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);

        hawq.createTableAndVerify(exTable);
    }

    @Override
    protected void queryResults() throws Exception {
        runTincTest("pxf.smoke.multi_block.runTest");
    }

    @Test(groups = "smoke")
    public void test() throws Exception {
        runTest();
    }
}
