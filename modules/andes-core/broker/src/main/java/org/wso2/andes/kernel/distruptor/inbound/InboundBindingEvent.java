/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.andes.kernel.distruptor.inbound;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.AndesBinding;
import org.wso2.andes.kernel.AndesContextInformationManager;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesQueue;

import static org.wso2.andes.kernel.distruptor.inbound.AndesInboundStateEvent.StateEvent.ADD_BINDING_EVENT;
import static org.wso2.andes.kernel.distruptor.inbound.AndesInboundStateEvent.StateEvent.REMOVE_BINDING_EVENT;

/**
 * Binding related inbound event 
 */
public class InboundBindingEvent extends AndesBinding implements AndesInboundStateEvent {

    private static Log log = LogFactory.getLog(InboundBindingEvent.class);

    /**
     * Reference to AndesContextInformationManager for add/remove binding
     */
    private AndesContextInformationManager contextInformationManager;

    /**
     * Andes binding related event type of this event 
     */
    private StateEvent eventType;
    
    public InboundBindingEvent(String boundExchangeName, AndesQueue boundQueue, String routingKey) {
        super(boundExchangeName, boundQueue, routingKey);
    }

    @Override
    public void updateState() throws AndesException {
        switch (eventType) {
            case ADD_BINDING_EVENT:
                contextInformationManager.createBinding(this);
                break;
            case REMOVE_BINDING_EVENT:
                contextInformationManager.removeBinding(this);
                break;
            default:
                log.error("Event type not set properly." + eventType);
                break;
        }
    }

    @Override
    public StateEvent getEventType() {
        return eventType;
    }

    /**
     * Update event to be an add binding event 
     * @param contextInformationManager AndesContextInformationManager
     */
    public void prepareForAddBindingEvent(AndesContextInformationManager contextInformationManager) {
        this.contextInformationManager = contextInformationManager;
        eventType = ADD_BINDING_EVENT;
    }

    /**
     * Update event to be a remove binding event  
     * @param contextInformationManager AndesContextInformationManager
     */
    public void prepareForRemoveBinding(AndesContextInformationManager contextInformationManager) {
        this.contextInformationManager = contextInformationManager;
        eventType = REMOVE_BINDING_EVENT;
    }
}
