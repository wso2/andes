package org.wso2.andes.kernel;

public interface SubscriptionListener {

	static enum SubscriptionChange{
        Added,
        Deleted,
        Disconnected}

    //TODO: There is no need of having this here since cluster wide subscription changes are handled by hazelcast.
    //Current cluster wide notification path is:
    // notifyLocalSubscriptionHasChanged >> send Hazelcast notification >> SubscriptionChangedListener is triggered >> hanlde
	public void notifyClusterSubscriptionHasChanged(Subscrption subscrption, SubscriptionChange changeType);

	public void notifyLocalSubscriptionHasChanged(LocalSubscription subscription, SubscriptionChange changeType);

}
