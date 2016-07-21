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

package org.wso2.andes.server.cluster.coordination;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.AndesContextInformationManager;
import org.wso2.andes.kernel.ClusterNotificationListener;
import org.wso2.andes.kernel.disruptor.inbound.InboundEventManager;
import org.wso2.andes.kernel.disruptor.inbound.InboundExchangeSyncEvent;

/**
 * ClusterNotificationListener implementation listening for message router changes
 * and handling them
 */
public class ExchangeNotificationHandler implements ClusterNotificationListener {

    private Log log = LogFactory.getLog(ExchangeNotificationHandler.class);

    /**
     * Manager for create/delete exchanges
     */
    private AndesContextInformationManager contextInformationManager;

    /**
     * Manager for publishing event to disruptor
     */
    private InboundEventManager inboundEventManager;

    /**
     * Create a listener to listen for message router changes in cluster
     *
     * @param contextInformationManager manager to handle notification inside Andes kernel
     * @param inboundEventManager       manager for generating inbound events
     */
    public ExchangeNotificationHandler(AndesContextInformationManager contextInformationManager,
                                       InboundEventManager inboundEventManager) {
        this.contextInformationManager = contextInformationManager;
        this.inboundEventManager = inboundEventManager;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handleClusterNotification(ClusterNotification notification) {
        try {
            InboundExchangeSyncEvent exchangeSyncEvent =
                    new InboundExchangeSyncEvent(notification.getEncodedObjectAsString());
            MessageRouterChange changeType = MessageRouterChange.valueOf(notification.getChangeType());
            switch (changeType) {
                case Added:
                    exchangeSyncEvent.prepareForCreateExchangeSync(contextInformationManager);
                    inboundEventManager.publishStateEvent(exchangeSyncEvent);
                    break;
                case Deleted:
                    exchangeSyncEvent.prepareForDeleteExchangeSync(contextInformationManager);
                    inboundEventManager.publishStateEvent(exchangeSyncEvent);
                    break;
            }
        } catch (Exception e) {
            log.error("Error while handling exchange notification", e);
        }
    }
}
