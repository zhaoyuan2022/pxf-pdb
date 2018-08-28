package com.pxf.automation.features;

import com.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import com.pxf.automation.BaseFunctionality;

public abstract class BaseFeature extends BaseFunctionality {

    protected void createTable(ReadableExternalTable hawqExternalTable) throws Exception {

        hawqExternalTable.setHost(pxfHost);
        hawqExternalTable.setPort(pxfPort);
        hawq.createTableAndVerify(hawqExternalTable);
    }

}