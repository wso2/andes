/*
 * Copyright (c) 2016, WSO2 Inc. (http://wso2.com) All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wso2.andes.kernel.registry;

import com.googlecode.cqengine.ConcurrentIndexedCollection;
import com.googlecode.cqengine.IndexedCollection;
import com.googlecode.cqengine.index.navigable.NavigableIndex;
import com.googlecode.cqengine.query.Query;
import org.wso2.andes.kernel.subscription.AndesSubscription;

import java.util.Iterator;

import static com.googlecode.cqengine.query.QueryFactory.equal;


/**
 * This is the subscription store. Whenever a subscription is
 * made it is registered here.
 * <p/>
 * 1. All local subscriptions with a physical connection to the node will be registered here
 * 2. All cluster subscriptions (which is actually connected to other nodes in cluster)
 * will also be registered here.
 * <p/>
 * Thus this will provide full view of subscription distribution in the cluster
 */
public class SubscriptionRegistry {

    /**
     * CQEngine based structure for keeping multi-indexed subscriptions
     * <p/>
     * | SubID   | Protocol | Protocol Version | RouterName | StorageQueueName | Routing Key |  Active/Inactive
     * | String  | Enum     | String           | String     | String           | String      |  boolean
     */
    private IndexedCollection<AndesSubscription> subscriptions;


    /**
     * Create a registry for keeping subscriptions in-memory. Introduce indexing needed.
     */
    public SubscriptionRegistry() {

        subscriptions = new ConcurrentIndexedCollection<>();

        subscriptions.addIndex(NavigableIndex.onAttribute(AndesSubscription.SUB_ID));
        subscriptions.addIndex(NavigableIndex.onAttribute(AndesSubscription.NODE_ID));
        subscriptions.addIndex(NavigableIndex.onAttribute(AndesSubscription.CHANNEL_ID));
        subscriptions.addIndex(NavigableIndex.onAttribute(AndesSubscription.PROTOCOL));
        subscriptions.addIndex(NavigableIndex.onAttribute(AndesSubscription.ROUTER_NAME));
        subscriptions.addIndex(NavigableIndex.onAttribute(AndesSubscription.STORAGE_QUEUE_NAME));
        subscriptions.addIndex(NavigableIndex.onAttribute(AndesSubscription.STATE));
        subscriptions.addIndex(NavigableIndex.onAttribute(AndesSubscription.ROUTING_KEY));

    }

    /**
     * Register subscription in registry
     *
     * @param subscription subscription to register
     */
    public void registerSubscription(AndesSubscription subscription) {
        subscriptions.add(subscription);
    }

    /**
     * Remove a subscription from registry
     *
     * @param subscription subscription to remove
     */
    public void removeSubscription(AndesSubscription subscription) {
        subscriptions.remove(subscription);
    }

    /**
     * Remove a subscription by ID
     *
     * @param subID ID of the subscription
     */
    public void removeSubscription(String subID) {

        Query<AndesSubscription> query = equal(AndesSubscription.SUB_ID, subID);
        for (AndesSubscription sub : subscriptions.retrieve(query)) {
            subscriptions.remove(sub);
        }
    }

    /**
     * Execute query and generate a Iterable with subscriptions matching to the query
     *
     * @param queryToRun query to run.
     * @return Iterable<AndesSubscription>
     */
    public Iterable<AndesSubscription> exucuteQuery(Query<AndesSubscription> queryToRun) {
        return subscriptions.retrieve(queryToRun);
    }

    /**
     * Get a list of subscriptions registered
     *
     * @return Iterator<AndesSubscription> on matching subscriptions
     */
    public Iterator<AndesSubscription> getAllSubscriptions() {
        return subscriptions.iterator();
    }

}
