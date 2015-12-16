package org.wso2.andes.subscription;

import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesSubscription;
import org.wso2.andes.kernel.DestinationType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Store all durable topic local subscriptions.
 * Subscriptions are stored against the target queue for durable topic use cases.
 */
public class LocalDurableTopicSubscriptionStore implements AndesSubscriptionStore {

    /**
     * Stores all durable subscriptions using the storage queue as the key.
     */
    private Map<String, Set<AndesSubscription>> durableTopicSubscriptionMap = new ConcurrentHashMap<>();

    /**
     * {@inheritDoc}
     */
    @Override
    public void addSubscription(AndesSubscription subscription) throws AndesException {
        String destination = subscription.getTargetQueue();

        Set<AndesSubscription> subscriptionSet = durableTopicSubscriptionMap.get(destination);

        if (null == subscriptionSet) {
            subscriptionSet = new HashSet<>();
            subscriptionSet.add(subscription);

            durableTopicSubscriptionMap.put(destination, subscriptionSet);
        } else {
            // If already available then update it
            if (subscriptionSet.contains(subscription)) {
                updateSubscription(subscription);
            } else {
                subscriptionSet.add(subscription);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateSubscription(AndesSubscription subscription) {
        Set<AndesSubscription> subscriptionSet = durableTopicSubscriptionMap.get(subscription.getTargetQueue());

        if (null != subscriptionSet) {
            subscriptionSet.remove(subscription);
            subscriptionSet.add(subscription);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isSubscriptionAvailable(AndesSubscription subscription) {
        boolean subscriptionAvailable = false;

        for (Map.Entry<String, Set<AndesSubscription>> entry : durableTopicSubscriptionMap.entrySet()) {
            if (entry.getValue().contains(subscription)) {
                subscriptionAvailable = true;
                break;
            }
        }

        return subscriptionAvailable;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeSubscription(AndesSubscription subscription) {
        Set<AndesSubscription> subscriptionSet = durableTopicSubscriptionMap.get(subscription.getTargetQueue());

        if (null != subscriptionSet) {
            subscriptionSet.remove(subscription);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<AndesSubscription> getMatchingSubscriptions(String destination, DestinationType destinationType) {
        Set<AndesSubscription> matchingSubscriptions = durableTopicSubscriptionMap.get(destination);

        if (null == matchingSubscriptions) {
            matchingSubscriptions = Collections.emptySet();
        }
        return matchingSubscriptions;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AndesSubscription> getAllSubscriptions() {
        List<AndesSubscription> subscriptionList = new ArrayList<>();

        for (Map.Entry<String, Set<AndesSubscription>> entry : durableTopicSubscriptionMap.entrySet()) {
            subscriptionList.addAll(entry.getValue());
        }

        return subscriptionList;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> getAllDestinations(DestinationType destinationType) {
        return durableTopicSubscriptionMap.keySet();
    }
}
