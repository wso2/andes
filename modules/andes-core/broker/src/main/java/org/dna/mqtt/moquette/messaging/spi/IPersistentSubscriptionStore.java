package org.dna.mqtt.moquette.messaging.spi;

import org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.Subscription;

import java.util.List;

public interface IPersistentSubscriptionStore {

    void addNewSubscription(Subscription newSubscription, String clientID);

    void removeAllSubscriptions(String clientID);

    List<Subscription> retrieveAllSubscriptions();
}
