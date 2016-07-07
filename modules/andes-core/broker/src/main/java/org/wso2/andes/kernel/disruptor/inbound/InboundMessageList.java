/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.andes.kernel.disruptor.inbound;

import org.wso2.andes.kernel.AndesChannel;
import org.wso2.andes.kernel.AndesMessage;

import java.util.ArrayList;
import java.util.List;

/**
 * This is a wrapper class for messageList in inbound event container.
 */
public class InboundMessageList  {

    private List<AndesMessage> messageList;

    InboundMessageList() {
        messageList = new ArrayList<AndesMessage>();
    }



    /**
     * [Important] two clear methods are needed as one for messaging event with channel already defined.
     * other is needed for non-messaging events where channel is not defined. This is required as
     * messages corresponding to a messaging event will be fully cleared from flow control buffer only upon end of
     * message event.
     *
     * @param channel andes channel
     */
    public void registerClear(AndesChannel channel) {
        channel.recordRemovalFromBuffer(getTotalChunkCount(messageList));
        clear();
    }

    /**
     * Clear inbound message list. This clear method should use for all non-message events when clear the event
     * container.
     */
    public void clear() {
        messageList.clear();

    }

    /**
     * Add message to inbound message list and add to flow control buffers.
     *
     * @param message andes message
     * @param channel andes channel
     */
    public void registerAddMessage(AndesMessage message, AndesChannel channel) {
        channel.recordAdditionToBuffer(message.getContentChunkList().size());
        messageList.add(message);
    }

    /**
     * Remove message from inbound message list and remove from flow control buffers.
     *
     * @param index index the index of the element to be removed
     * @param channel andes channel
     * @return andes message
     */
    public AndesMessage registerPopMessage(int index, AndesChannel channel) {
        AndesMessage message = messageList.remove(index);
        channel.recordRemovalFromBuffer(message.getContentChunkList().size());
        return message;
    }

    /**
     * This returns andes message list from inbound message list.
     * @return andes message list
     */
    public List<AndesMessage> getMessageList() {
        return messageList;
    }

    /**
     * Get total number of content chunks present in provided message list
     * @param messages AndesMessage list
     * @return total message chunks
     */
    private static int getTotalChunkCount(List<AndesMessage> messages) {
        int count = 0;
        for (AndesMessage message : messages) {
            count = count + message.getContentChunkList().size();
        }
        return count;
    }
}
