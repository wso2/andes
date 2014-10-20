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

import org.dna.mqtt.wso2.AndesBridge;
import org.wso2.andes.kernel.*;
import org.wso2.andes.server.ClusterResourceHolder;

import java.nio.ByteBuffer;


public class MQTTChannel {

    private static MQTTChannel channel;
    //private AndesBridge tem_bridge;

    /*
    *Will produce a new instance
    * Wil interface betweeen the kernal and the MQTT library*/
    public static synchronized MQTTChannel getInstance() {
        if (channel == null) {
            channel = new MQTTChannel();
        }

        return channel;
    }

    /*
    * Will insert the message body to the kernal*/
    private void addMessageBody(AndesMessagePart messageBody) {
        MessagingEngine.getInstance().messageContentReceived(messageBody);
    }

    /*Will insert the message header to the kernal*/
    private void addMessageHeader(AndesMessageMetadata messageHeader) {
        try {
            MessagingEngine.getInstance().messageReceived(messageHeader);
        } catch (AndesException e) {
            e.printStackTrace();
        }
    }

    /*Will get the message from the MQTT library*/
    public void handleMessageInstance(ByteBuffer pub_message, long msgID, String topic) {
        ByteBuffer message = pub_message;
        long messageID = msgID;
        //Will start converting the message body
        // AndesMessagePart msg = convertMessageBody(message,messageID);
        AndesMessagePart msg = MQTTUtils.convertToAndesMessage(message, messageID);
        //Will Create the Andes Header
        AndesMessageMetadata metaHeader = MQTTUtils.convertToAndesHeader(messageID, topic, pub_message.array().length);
        //Will write the message body
        addMessageBody(msg);
        //Will add the message header
        addMessageHeader(metaHeader);
    }

    /*Will handle the subscription
    * Keeps the server channel*/
    public void insertSubscriber(AndesBridge channel, String topic, String clientID) {
        //To-DO Need to check whether subscription to the topic has being created

        //Will create a new local subscription object
        MQTTLocalSubscription localSubscription = new MQTTLocalSubscription("");
        localSubscription.setMqqtServerChannel(channel);
        localSubscription.setTopic(topic);
        localSubscription.setSubscriptionID(clientID);
        localSubscription.setIsActive(true);

        //Shold indicate the record in the cluster
        try {
            ClusterResourceHolder.getInstance().getSubscriptionManager().addSubscription(localSubscription);
        } catch (AndesException e) {
            e.printStackTrace();
        }
    }

    /*Will handle subscriber disconnection*/
    public void disconnectSubscriber(AndesBridge channel, String subscribedTopic, String clientID) {
        try {

            //Will create a new local subscription object
            MQTTLocalSubscription localSubscription = new MQTTLocalSubscription("");
            localSubscription.setMqqtServerChannel(channel);
            localSubscription.setTopic(subscribedTopic);
            localSubscription.setSubscriptionID(clientID);
            localSubscription.setIsActive(false);

            ClusterResourceHolder.getInstance().getSubscriptionManager().closeLocalSubscription(localSubscription);

        } catch (AndesException e) {
            e.printStackTrace();
        }
    }


}
