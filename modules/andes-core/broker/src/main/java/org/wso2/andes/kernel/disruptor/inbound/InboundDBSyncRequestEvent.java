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

package org.wso2.andes.kernel.disruptor.inbound;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.AMQPConstructStore;
import org.wso2.andes.kernel.AndesBinding;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.kernel.AndesContextInformationManager;
import org.wso2.andes.kernel.AndesContextStore;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.router.AndesMessageRouter;
import org.wso2.andes.kernel.subscription.AndesSubscriptionManager;
import org.wso2.andes.kernel.subscription.StorageQueue;

import java.util.ArrayList;
import java.util.List;

/**
 * DB sync requests are handled through this event
 */
public class InboundDBSyncRequestEvent implements AndesInboundStateEvent {

    private static Log log = LogFactory.getLog(InboundDBSyncRequestEvent.class);

    /**
     * Type of this event
     */
    private static final String EVENT_TYPE = "DB_SYNC";

    /**
     * Store keeping control information of broker
     */
    private AndesContextStore andesContextStore;

    /**
     * Manager instance to modify message routers, queues, bindings etc
     */
    private AndesContextInformationManager contextInformationManager;

    /**
     * Manager instance to create/delete subscriptions
     */
    private AndesSubscriptionManager subscriptionManager;

    /**
     * AMQP specific artifact store
     */
    private AMQPConstructStore amqpConstructStore;


    /**
     * {@inheritDoc}
     */
    @Override
    public void updateState() throws AndesException {
        log.info("Running DB sync task.");
        reloadMessageRoutersFromDB();
        reloadQueuesFromDB();
        reloadBindingsFromDB();
        reloadSubscriptions();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String eventInfo() {
        return EVENT_TYPE;
    }

    /**
     * Prepare event for publishing
     *
     * @param contextStore              store keeping control information of broker
     * @param amqpConstructStore        AMQP specific artifact store
     * @param contextInformationManager manager instance to modify message routers, queues, bindings etc
     * @param subscriptionManager       manager instance to create/delete subscriptions
     */
    public void prepareEvent(AndesContextStore contextStore, AMQPConstructStore amqpConstructStore,
                             AndesContextInformationManager contextInformationManager,
                             AndesSubscriptionManager subscriptionManager) {
        this.andesContextStore = contextStore;
        this.contextInformationManager = contextInformationManager;
        this.subscriptionManager = subscriptionManager;
        this.amqpConstructStore = amqpConstructStore;
    }

    /**
     * Reload message routers from DB. Delete information in memory that is not
     * stored in DB, and add information in DB but not in memory.
     *
     * @throws AndesException
     */
    private void reloadMessageRoutersFromDB() throws AndesException {
        List<AndesMessageRouter> messageRoutersStored = andesContextStore.getAllMessageRoutersStored();
        List<AndesMessageRouter> messageRoutersInMemory = AndesContext.getInstance().
                getMessageRouterRegistry().getAllMessageRouters();
        List<AndesMessageRouter> copyOfMessageRoutersStored = new ArrayList<>(messageRoutersStored);

        messageRoutersStored.removeAll(messageRoutersInMemory);
        for (AndesMessageRouter messageRouter : messageRoutersStored) {
            log.warn("Recovering node. Adding exchange " + messageRouter.toString());
            InboundExchangeSyncEvent exchangeCreateEvent =
                    new InboundExchangeSyncEvent(messageRouter.encodeAsString());
            exchangeCreateEvent.prepareForCreateExchangeSync(contextInformationManager);
            exchangeCreateEvent.updateState();
        }

        messageRoutersInMemory.removeAll(copyOfMessageRoutersStored);
        for (AndesMessageRouter messageRouter : messageRoutersInMemory) {
            log.warn("Recovering node. Removing exchange " + messageRouter.toString());
            InboundExchangeSyncEvent exchangeDeleteEvent =
                    new InboundExchangeSyncEvent(messageRouter.encodeAsString());
            exchangeDeleteEvent.prepareForDeleteExchangeSync(contextInformationManager);
            exchangeDeleteEvent.updateState();
        }
    }

    /**
     * Reload queues from DB. Delete information in memory that is not
     * stored in DB, and add information in DB but not in memory.
     *
     * @throws AndesException
     */
    private void reloadQueuesFromDB() throws AndesException {
        List<StorageQueue> queuesStored = andesContextStore.getAllQueuesStored();
        List<StorageQueue> queuesInMemory = AndesContext.getInstance().getStorageQueueRegistry()
                .getAllStorageQueues();
        List<StorageQueue> copyOfQueuesStored = new ArrayList<>(queuesStored);

        queuesStored.removeAll(queuesInMemory);
        for (StorageQueue queue : queuesStored) {
            log.warn("Recovering node. Adding queue to queue registry " + queue.toString());
            InboundQueueSyncEvent queueCreateEvent = new InboundQueueSyncEvent(queue.encodeAsString());
            queueCreateEvent.prepareForSyncCreateQueue(contextInformationManager);
            queueCreateEvent.updateState();
        }

        queuesInMemory.removeAll(copyOfQueuesStored);
        for (StorageQueue queue : queuesInMemory) {
            log.warn("Recovering node. Removing queue from queue registry " + queue.toString());
            InboundQueueSyncEvent queueDeleteEvent = new InboundQueueSyncEvent(queue.encodeAsString());
            queueDeleteEvent.prepareForSyncDeleteQueue(contextInformationManager);
            queueDeleteEvent.updateState();
        }

    }

    /**
     * Reload bindings from DB. Delete information in memory that is not
     * stored in DB, and add information in DB but not in memory.
     *
     * @throws AndesException
     */
    private void reloadBindingsFromDB() throws AndesException {
        List<AndesMessageRouter> routersStored = andesContextStore.getAllMessageRoutersStored();
        for (AndesMessageRouter messageRouter : routersStored) {
            List<AndesBinding> bindingsStored =
                    andesContextStore.getBindingsStoredForExchange(messageRouter.getName());
            List<AndesBinding> inMemoryBindings =
                    amqpConstructStore.getBindingsForExchange(messageRouter.getName());
            List<AndesBinding> copyOfBindingsStored = new ArrayList<>(bindingsStored);

            bindingsStored.removeAll(inMemoryBindings);
            for (AndesBinding binding : bindingsStored) {
                log.warn("Recovering node. Adding binding " + binding.toString());
                InboundBindingSyncEvent bindingCreateEvent =
                        new InboundBindingSyncEvent(binding.encodeAsString());
                bindingCreateEvent.prepareForAddBindingEvent(contextInformationManager);
                bindingCreateEvent.updateState();
            }

            inMemoryBindings.removeAll(copyOfBindingsStored);
            for (AndesBinding binding : inMemoryBindings) {
                log.warn("Recovering node. removing binding " + binding.toString());
                InboundBindingSyncEvent bindingDeleteEvent =
                        new InboundBindingSyncEvent(binding.encodeAsString());
                bindingDeleteEvent.prepareForRemoveBinding(contextInformationManager);
                bindingDeleteEvent.updateState();
            }
        }

    }

    /**
     * Reload subscriptions from DB. Delete information in memory that is not
     * stored in DB, and add information in DB but not in memory.
     *
     * @throws AndesException
     */
    private void reloadSubscriptions() throws AndesException {
        subscriptionManager.reloadSubscriptionsFromStorage();

    }
}
