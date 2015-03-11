package org.wso2.andes.store;


/**
 * Defines contractual obligations for any store which allows its users to know
 * about its operational status
 */
public interface HealthAwareStore {

    /**
     * Determine weather connection is healthy
     * 
     * @param testString a String value to write to database.
     * @param testTime a time value ( ideally the current time)
     * @return true of message store is operational.
     */
    public boolean isOperational(String testString, long testTime);
}
