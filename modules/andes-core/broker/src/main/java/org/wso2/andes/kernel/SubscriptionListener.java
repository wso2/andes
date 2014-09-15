package org.wso2.andes.kernel;

public interface SubscriptionListener {

	static enum SubscriptionChange{
        Added,
        Deleted,
        Disconnected}

    /**
     * handle subscription changes in cluster
     * @param subscription subscription changed
     * @param changeType type of change happened
     * @throws AndesException
     */
	public void handleClusterSubscriptionsChanged(AndesSubscription subscription, SubscriptionChange changeType) throws AndesException;

    /**
     * handle local subscription changes
     * @param subscription subscription changed
     * @param changeType type of change happened
     * @throws AndesException
     */
	public void handleLocalSubscriptionsChanged(LocalSubscription subscription, SubscriptionChange changeType) throws AndesException;

}
