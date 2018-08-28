package com.pxf.automation.utils.system;

/***
 * Utility for working with system-wide parameters
 */
public abstract class SystemUtils {

    public static final String PGMODE_PROPERTY_NAME = "PG_MODE";
    public static PGModeEnum getPGMode() {

        PGModeEnum result;
        try {
            result = PGModeEnum.valueOf(System.getProperty(PGMODE_PROPERTY_NAME, PGModeEnum.HAWQ.name()).toUpperCase());
        } catch (Exception e) {
            result = PGModeEnum.HAWQ; // use HAWQ as default mode
        }

        return result;
    }
}
