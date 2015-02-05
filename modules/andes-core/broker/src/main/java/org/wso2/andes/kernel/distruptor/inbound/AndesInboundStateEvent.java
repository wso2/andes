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

import org.wso2.andes.kernel.AndesException;

/**
 * Andes Inbound event types that only update state implements this interface 
 */
public interface AndesInboundStateEvent {

    /**
     * Supported state events by Andes 
     */
    public enum StateEvent {
        /** Specific client channel close event */
        CHANNEL_CLOSE_EVENT,

        /** New client connected and client channel is opened event */
        CHANNEL_OPEN_EVENT,

        /** Stop message delivery in Andes core */
        STOP_MESSAGE_DELIVERY_EVENT,

        /** Start message delivery in Andes core event */
        START_MESSAGE_DELIVERY_EVENT,

        /** Shutdown andes broker messaging engine event*/
        SHUTDOWN_MESSAGING_ENGINE_EVENT,

        /** New local subscription created event */
        OPEN_SUBSCRIPTION_EVENT,

        /** Close a local subscription event */
        CLOSE_SUBSCRIPTION_EVENT,

        /** Start expired message deleting task, notification event */
        START_EXPIRATION_WORKER_EVENT,

        /** Stop expired message deleting task, notification event */
        STOP_EXPIRATION_WORKER_EVENT,

        /** Queue purging event related event type */
        QUEUE_PURGE_EVENT,
        
        /** Is queue deletable check related event type */
        IS_QUEUE_DELETABLE_EVENT,

        /** Delete messages event related event type*/
        DELETE_MESSAGES_EVENT,

        /** Delete the queue from DB related event type */
        DELETE_QUEUE_EVENT,

        /** Create a queue in Andes related event type */
        CREATE_QUEUE_EVENT,

        /** Create a binding in Andes related event type */
        ADD_BINDING_EVENT,

        /** Delete a binding in Andes related event type */
        REMOVE_BINDING_EVENT, 
        
        /** Create exchange in Andes related event type */
        CREATE_EXCHANGE_EVENT,
        
        /** Delete exchange in Andes related event type */
        DELETE_EXCHANGE_EVENT
    } 
    
    /**
     * Updates Andes internal state according to the inbound event. 
     * Method visibility is package specific. Only StateEventHandler should call this 
     * @throws AndesException
     */
    void updateState() throws AndesException;

    /**
     * The state event type handled by the event 
     * @return StateEvent
     */
    StateEvent getEventType();
}
