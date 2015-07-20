/*
 * Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.andes.kernel;

import java.util.UUID;

public interface LocalSubscription extends AndesSubscription {

    public void sendMessageToSubscriber(AndesMessageMetadata messageMetadata, AndesContent content)throws AndesException;

    public boolean isActive();

    public UUID getChannelID();

    /**
     * Check if this subscription has ability to accept messages
     * If pending ack count is high it does not have ability to accept new messages
     * @return true if able to accept messages
     */
    public boolean hasRoomToAcceptMessages();

    /**
     * Ack received for subscription. Protocol specific subscribers implement
     * independent behaviours
     * @param messageID id of the message
     */
    public void ackReceived(long messageID);

    /**
     * Message rejected by the subscriber. Protocol specific subscribers implement
     * independent behaviours
     * @param messageID id of the rejected message
     */
    public void msgRejectReceived(long messageID);

    /**
     * Close subscriber. Here subscriber should release all the resources.
     */
    public void close();

    public LocalSubscription createQueueToListentoTopic();
}
