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
 * Inbound event for disruptor representing a message router change notification
 */
public class InboundExchangeSyncEvent implements AndesInboundStateEvent {

    private static Log log = LogFactory.getLog(InboundExchangeSyncEvent.class);

    /**
     * Internal enum representing possibilities of message router
     * change
     */
    private enum EventType {

        SYNC_EXCHANGE_CREATE_EVENT,

        SYNC_EXCHANGE_DELETE_EVENT
    }

    /**
     * Event type this event
     */
    private EventType eventType;

    /**
     * Exchange as an encoded string
     */
    private String encodedExchangeInfo;

    /**
     * Reference to AndesContextInformationManager to update create/ remove exchanges
     */
    private AndesContextInformationManager contextInformationManager;


    /**
     * Create a event representing a message router change
     *
     * @param exchangeInfo encoded information of a
     *                     {@link org.wso2.andes.kernel.router.AndesMessageRouter}
     */
    public InboundExchangeSyncEvent(String exchangeInfo) {
        this.encodedExchangeInfo = exchangeInfo;
    }

    /**
     * Get string representation of a {@link org.wso2.andes.kernel.router.AndesMessageRouter}
     *
     * @return String with message router information
     */
    public String getEncodedExchangeInfo() {
        return encodedExchangeInfo;
    }

    @Override
    public void updateState() throws AndesException {
        switch (eventType) {
            case SYNC_EXCHANGE_CREATE_EVENT:
                contextInformationManager.syncExchangeCreate(this);
                break;
            case SYNC_EXCHANGE_DELETE_EVENT:
                contextInformationManager.syncExchangeDelete(this);
                break;
            default:
                log.error("Event type not set properly " + eventType);
                break;
        }
    }

    @Override
    public String eventInfo() {
        return this.eventType.toString();
    }


    /**
     * Update the event to a create exchange sync event
     *
     * @param contextInformationManager AndesContextInformationManager
     */
    public void prepareForCreateExchangeSync(AndesContextInformationManager contextInformationManager) {
        eventType = EventType.SYNC_EXCHANGE_CREATE_EVENT;
        this.contextInformationManager = contextInformationManager;
    }

    /**
     * Update event to delete exchange sync event
     *
     * @param contextInformationManager AndesContextInformationManager
     */
    public void prepareForDeleteExchangeSync(AndesContextInformationManager contextInformationManager) {
        eventType = EventType.SYNC_EXCHANGE_DELETE_EVENT;
        this.contextInformationManager = contextInformationManager;
    }

}
