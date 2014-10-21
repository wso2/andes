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

package org.wso2.andes.mqtt;

import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.AndesMessagePart;
import org.wso2.andes.kernel.MessagingEngine;

import java.nio.ByteBuffer;


public class MQTTUtils {


    /*Will convert an MQTT Message complient object to Andes*/
    public static AndesMessagePart convertToAndesMessage(ByteBuffer message, long messagID) {
        AndesMessagePart messageBody = new AndesMessagePart();
        byte[] data = message.array();

        messageBody.setOffSet(0);
        messageBody.setData(data);
        messageBody.setMessageID(messagID);
        messageBody.setDataLength(data.length);
        return messageBody;
    }

    /*Will generate a unique message ID*/
    public static long generateMessageID() {
        return MessagingEngine.getInstance().generateNewMessageId();
    }

    /*Will construct the message meata information*/
    public static byte[] ConstructMetaInformation(String meta_content, long messageID, boolean topic, String destination, boolean persistance, int contentLength) {
        byte[] meta_information = null;
        String information = meta_content + ":MessageID=" + messageID + ",Topic=" + topic +
                ",Destination=" + destination + ",Persistant=" + persistance + ",MessageContentLength=" + contentLength;
        meta_information = information.getBytes();
        return meta_information;
    }

    /*Will convert the message content header to andes meta data*/
    public static AndesMessageMetadata convertToAndesHeader(long messageID, String topic, int messageContentLength) {
        //Setting up meta-data
        String dummy_meta_info = "\u0002Dummy MQQT Information";
        AndesMessageMetadata messageHeader = new AndesMessageMetadata();
        messageHeader.setMessageID(messageID);
        messageHeader.setTopic(true);
        messageHeader.setDestination(topic);
        messageHeader.setPersistent(true);
        messageHeader.setChannelId(1);
        messageHeader.setMessageContentLength(messageContentLength);

        byte[] meta_data = ConstructMetaInformation(dummy_meta_info, messageHeader.getMessageID(), messageHeader.isTopic(),
                messageHeader.getDestination(), messageHeader.isPersistent(), messageContentLength);

        messageHeader.setMetadata(meta_data);
        return messageHeader;
    }

    /*Will get the message content from meta information*/
    public static ByteBuffer getContentFromMetaInformation(AndesMessageMetadata metadata) {
        //Need to get the value dynamically
        ByteBuffer message = ByteBuffer.allocate(metadata.getMessageContentLength());
        try {
            AndesMessagePart messagePart = MessagingEngine.getInstance().getContent(metadata.getMessageID(), 0);
            message.put(messagePart.getData());
        } catch (AndesException e) {
            throw new RuntimeException("Error in getting content for message", e);
        }
        return message;
    }

    /*Will generate id for a connection*/
    public static String generateBridgingClientID() {
        return "MQTTAndesSub:" + String.valueOf(MessagingEngine.getInstance().generateNewMessageId());
    }
}
