package com.pxf.automation.testplugin;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hawq.pxf.api.OneRow;
import org.apache.hawq.pxf.api.ReadAccessor;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.api.utilities.Plugin;

/**
 * Test class for regression tests.
 * The only thing this class does is to throw an exception
 * containing the received filter from HAWQ (HAS-FILTER & FILTER).
 */
public class FilterPrinterAccessor extends Plugin implements ReadAccessor
{
    static private Log Log = LogFactory.getLog(FilterPrinterAccessor.class);

    /*
     * exception for exposing the filter to the world
     */
    class FilterPrinterException extends Exception {
        FilterPrinterException(String filter) {
            super("Filter string: '" + filter + "'");
        }
    }

    public FilterPrinterAccessor(InputData input) {
        super(input);
    }

    @Override
    public boolean openForRead() throws Exception {

        String filter = inputData.hasFilter() ?
                inputData.getFilterString() : "No filter";

        throw new FilterPrinterException(filter);
    }

    @Override
    public void closeForRead() {
        throw new UnsupportedOperationException("closeForRead is not implemented");
    }

    @Override
    public OneRow readNextObject() {
        throw new UnsupportedOperationException("readNextObject is not implemented");
    }
}