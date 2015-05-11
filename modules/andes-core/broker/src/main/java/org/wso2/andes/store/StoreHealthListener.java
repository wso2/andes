package org.wso2.andes.store;

/**
 * Defines contractual obligations for any party interested in knowing
 * when a {@link HealthAwareStore} becomes operational or offline.
 *
 */
public interface StoreHealthListener {

    /**
     * Invoked when specified store becomes in-operational
     * 
     * @param store
     *            the store which went offline.
     * @param ex
     *            exception occurred.
     */
    public void storeInoperational(HealthAwareStore store, Exception ex);

    /**
     * Invoked when specified store becomes operational
     * 
     * @param store
     *            Reference to the operational store
     */
    public void storeOperational(HealthAwareStore store);

}
