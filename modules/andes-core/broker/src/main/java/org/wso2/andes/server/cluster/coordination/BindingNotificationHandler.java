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
import org.wso2.andes.kernel.disruptor.inbound.InboundBindingSyncEvent;
import org.wso2.andes.kernel.disruptor.inbound.InboundEventManager;

/**
 * ClusterNotificationListener implementation listening for binding changes
 * and handling them
 */
public class BindingNotificationHandler implements ClusterNotificationListener {

    private Log log = LogFactory.getLog(BindingNotificationHandler.class);
    private AndesContextInformationManager contextInformationManager;
    private InboundEventManager inboundEventManager;

    /**
     * Create a listener to listen for binding changes in cluster
     *
     * @param contextInformationManager manager to handle notification inside Andes kernel
     * @param inboundEventManager       manager for generating inbound events
     */
    public BindingNotificationHandler(AndesContextInformationManager contextInformationManager,
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
            InboundBindingSyncEvent bindingSyncEvent = new InboundBindingSyncEvent
                    (notification.getEncodedObjectAsString());
            BindingChange changeType = BindingChange.valueOf(notification.getChangeType());
            switch (changeType) {
                case Added:
                    bindingSyncEvent.prepareForAddBindingEvent(contextInformationManager);
                    inboundEventManager.publishStateEvent(bindingSyncEvent);
                    break;
                case Deleted:
                    bindingSyncEvent.prepareForRemoveBinding(contextInformationManager);
                    inboundEventManager.publishStateEvent(bindingSyncEvent);
                    break;
            }
        } catch (Exception e) {
            log.error("Error while handling binding notification", e);
        }
    }
}
