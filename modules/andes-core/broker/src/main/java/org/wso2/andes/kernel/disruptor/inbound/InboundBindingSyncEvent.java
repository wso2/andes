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
import org.wso2.andes.kernel.AndesContextInformationManager;
import org.wso2.andes.kernel.AndesException;

/**
 * Inbound event for disruptor representing a binding change notification
 */
public class InboundBindingSyncEvent implements AndesInboundStateEvent {

    private static Log log = LogFactory.getLog(InboundBindingEvent.class);

    /**
     * Supported state events
     */
    private enum EventType {

        /**
         * Create a binding in Andes related event type
         */
        SYNC_BINDING_ADD_EVENT,

        /**
         * Delete a binding in Andes related event type
         */
        SYNC_BINDING_REMOVE_EVENT,
    }

    /**
     * Reference to AndesContextInformationManager for add/remove binding
     */
    private AndesContextInformationManager contextInformationManager;

    /**
     * Andes binding related event type of this event
     */
    private EventType eventType;


    private String encodedBindingInfo;

    /**
     * Create a event representing a binding change
     *
     * @param encodedBindingInfo encoded information of
     *                           a {@link org.wso2.andes.kernel.AndesBinding}
     */
    public InboundBindingSyncEvent(String encodedBindingInfo) {
        this.encodedBindingInfo = encodedBindingInfo;
    }

    /**
     * Get string representation of
     * a {@link org.wso2.andes.kernel.AndesBinding}
     *
     * @return String with binding information
     */
    public String getEncodedBindingInfo() {
        return encodedBindingInfo;
    }

    /**
     * {@inheritDoc}
     *
     * @throws AndesException
     */
    @Override
    public void updateState() throws AndesException {
        switch (eventType) {
            case SYNC_BINDING_ADD_EVENT:
                contextInformationManager.syncCreateBinding(this);
                break;
            case SYNC_BINDING_REMOVE_EVENT:
                contextInformationManager.syncRemoveBinding(this);
                break;
            default:
                log.error("Event type not set properly." + eventType);
                break;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String eventInfo() {
        return eventType.toString();
    }

    /**
     * Update event to be an add binding event
     *
     * @param contextInformationManager AndesContextInformationManager
     */
    public void prepareForAddBindingEvent(AndesContextInformationManager contextInformationManager) {
        this.contextInformationManager = contextInformationManager;
        eventType = EventType.SYNC_BINDING_ADD_EVENT;
    }

    /**
     * Update event to be a remove binding event
     *
     * @param contextInformationManager AndesContextInformationManager
     */
    public void prepareForRemoveBinding(AndesContextInformationManager contextInformationManager) {
        this.contextInformationManager = contextInformationManager;
        eventType = EventType.SYNC_BINDING_REMOVE_EVENT;
    }
}
