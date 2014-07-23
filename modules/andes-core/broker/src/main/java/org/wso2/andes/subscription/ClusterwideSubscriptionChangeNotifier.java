package org.wso2.andes.subscription;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.LocalSubscription;
import org.wso2.andes.kernel.SubscriptionListener;
import org.wso2.andes.kernel.Subscrption;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.cluster.coordination.SubscriptionNotification;

public class ClusterwideSubscriptionChangeNotifier implements SubscriptionListener{
	private static Log log = LogFactory.getLog(OrphanedMessagesDueToUnsubscriptionHandler.class);

	@Override
	public void notifyClusterSubscriptionHasChanged(Subscrption subscrption,
			SubscriptionChange changeType) {
        //TODO: hasitha - what abt resetting the global queue workers running??
	}

	@Override
	public void notifyLocalSubscriptionHasChanged(
			LocalSubscription subscription, SubscriptionChange changeType) {
        SubscriptionNotification subscriptionNotification = new SubscriptionNotification(
                subscription.getTargetQueueBoundExchangeName(),
                subscription.getTargetQueueBoundExchangeType(),
                subscription.ifTargetQueueBoundExchangeAutoDeletable(),
                changeType,
                subscription.getTargetQueue(),
                subscription.getTargetQueueOwner(),
                subscription.isExclusive(),
                subscription.isDurable(),
                subscription.getSubscribedDestination(),
                subscription.encodeAsStr());

        ClusterResourceHolder.getInstance().getSubscriptionCoordinationManager().handleLocalSubscriptionChange(subscriptionNotification);
    }
}
