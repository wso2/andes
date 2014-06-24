package org.wso2.andes.kernel;

public interface SubscriptionListener {

	static enum SubscriptionChange{
        Added,
        Deleted,
        Disconnected}

	public void notifyClusterSubscriptionHasChanged(Subscrption subscrption, SubscriptionChange changeType);

	public void notifyLocalSubscriptionHasChanged(LocalSubscription subscription, SubscriptionChange changeType);

}
