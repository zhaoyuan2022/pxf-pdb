package com.pxf.automation.testplugin;

import java.util.List;

import org.apache.hawq.pxf.api.Fragment;
import org.apache.hawq.pxf.api.Fragmenter;
import org.apache.hawq.pxf.api.utilities.InputData;

/**
 * Test class for regression tests.
 * The only thing this class does is to take received filter string from HAWQ (HAS-FILTER & FILTER).
 * And return it in UserData back to hawq for later validation in Resolver/Accessor
 */
public class FilterVerifyFragmenter extends Fragmenter
{
    public FilterVerifyFragmenter(InputData input) {
        super(input);
    }

    /**
     * Returns one fragment with incoming filter string value as the user data.
     * If no incoming filter, then return "No Filter" as user data.
     *
     * @return one data fragment
     * @throws Exception
     */
    @Override
    public List<Fragment> getFragments() throws Exception {

        String filter = (inputData.hasFilter() ?
                inputData.getFilterString() : "No filter");

        String [] hosts =  {"localhost" , "localhost" , "localhost"};

        // Set filter value as returned user data.
        Fragment fragment = new Fragment("dummy_file_path",
                hosts,
                new String().getBytes(),
                filter.getBytes());
        fragments.add(fragment);

        return fragments;
    }
}