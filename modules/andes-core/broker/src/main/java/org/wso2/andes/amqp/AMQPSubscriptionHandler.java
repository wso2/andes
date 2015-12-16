package org.wso2.andes.amqp;

import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesSubscription;
import org.wso2.andes.kernel.DestinationType;
import org.wso2.andes.kernel.ProtocolType;
import org.wso2.andes.subscription.SubscriptionHandler;
import org.wso2.andes.subscription.TopicSubscriptionBitMapHandler;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class AMQPSubscriptionHandler implements SubscriptionHandler {


    /**
     * All topic subscriptions will be handled through this.
     */
    SubscriptionHandler topicSubscriptionHandler;

    /**
     * All queue subscriptions will go into this.
     */
    private Map<String, Set<AndesSubscription>> queueSubscriptionMap = new ConcurrentHashMap<>();

    public AMQPSubscriptionHandler() throws AndesException {
        topicSubscriptionHandler = new TopicSubscriptionBitMapHandler(ProtocolType.AMQP);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addSubscription(AndesSubscription subscription) throws AndesException {
        if (DestinationType.TOPIC == subscription.getDestinationType()) {
            topicSubscriptionHandler.addSubscription(subscription);

            if (subscription.isDurable()) {
                addQueueSubscription(subscription);
            }
        } else {
            addQueueSubscription(subscription);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateSubscription(AndesSubscription subscription) {
        if (DestinationType.TOPIC == subscription.getDestinationType()) {
            topicSubscriptionHandler.updateSubscription(subscription);

            if (subscription.isDurable()) {
                updateQueueSubscription(subscription);
            }
        } else {
            updateQueueSubscription(subscription);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isSubscriptionAvailable(AndesSubscription subscription) {
        boolean subscriptionAvailable = false;

        subscriptionAvailable = topicSubscriptionHandler.isSubscriptionAvailable(subscription);

        if (!subscriptionAvailable) {
            for (Map.Entry<String, Set<AndesSubscription>> entry : queueSubscriptionMap.entrySet()) {
                if (entry.getValue().contains(subscription)) {
                    subscriptionAvailable = true;
                    break;
                }
            }
        }

        return subscriptionAvailable;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeSubscription(AndesSubscription subscription) {
        if (DestinationType.TOPIC == subscription.getDestinationType()) {
            topicSubscriptionHandler.removeSubscription(subscription);

            if (subscription.isDurable()) {
                removeQueueSubscriptions(subscription);
            }
        } else {
            removeQueueSubscriptions(subscription);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<AndesSubscription> getMatchingSubscriptions(String destination, DestinationType destinationType) {
        Set<AndesSubscription> matchingSubscriptions;
        if (DestinationType.TOPIC == destinationType) {
            matchingSubscriptions = topicSubscriptionHandler.getMatchingSubscriptions(destination, destinationType);
        } else {
            matchingSubscriptions = queueSubscriptionMap.get(destination);
        }

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

        subscriptionList.addAll(topicSubscriptionHandler.getAllSubscriptions());

        for (Map.Entry<String, Set<AndesSubscription>> entry : queueSubscriptionMap.entrySet()) {
            subscriptionList.addAll(entry.getValue());
        }

        return subscriptionList;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> getAllDestinations(DestinationType destinationType) {
        Set<String> destinations;

        if (DestinationType.TOPIC == destinationType) {
            destinations = topicSubscriptionHandler.getAllDestinations(destinationType);
        } else {
            destinations = queueSubscriptionMap.keySet();
        }

        return destinations;
    }

    /**
     * Add a new queue subscription to the underlying data structure.
     *
     * @param subscription The subscription to add
     */
    private void addQueueSubscription(AndesSubscription subscription) {
        String destination = subscription.getSubscribedDestination();

        Set<AndesSubscription> subscriptionSet = queueSubscriptionMap.get(destination);

        if (null == subscriptionSet) {
            subscriptionSet = new HashSet<>();
            subscriptionSet.add(subscription);

            queueSubscriptionMap.put(destination, subscriptionSet);
        } else {
            // If already available then update it
            if (subscriptionSet.contains(subscription)) {
                updateQueueSubscription(subscription);
            } else {
                subscriptionSet.add(subscription);
            }
        }
    }

    /**
     * Update an already available queue subscription object.
     *
     * @param subscription The subscription to update
     */
    private void updateQueueSubscription(AndesSubscription subscription) {
        String destination;

        if (DestinationType.TOPIC == subscription.getDestinationType()) { // This is a durable topic subscription
            destination = subscription.getTargetQueue();
        } else {
            destination = subscription.getSubscribedDestination();
        }

        Set<AndesSubscription> subscriptionSet = queueSubscriptionMap.get(destination);

        if (null != subscriptionSet) {
            subscriptionSet.remove(subscription);
            subscriptionSet.add(subscription);
        }
    }

    /**
     * Remove a queue subscription.
     *
     * @param subscription The subscription to remove
     */
    private void removeQueueSubscriptions(AndesSubscription subscription) {
        String destination = subscription.getSubscribedDestination();

        Set<AndesSubscription> subscriptionSet = queueSubscriptionMap.get(destination);

        if (null != subscriptionSet) {
            subscriptionSet.remove(subscription);
        }
    }
}
