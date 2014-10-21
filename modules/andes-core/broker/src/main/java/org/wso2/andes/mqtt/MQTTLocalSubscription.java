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

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.dna.mqtt.wso2.AndesBridge;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.LocalSubscription;
import org.wso2.andes.server.util.AndesUtils;
import org.wso2.andes.subscription.BasicSubscription;

import java.nio.ByteBuffer;


public class MQTTLocalSubscription extends BasicSubscription implements LocalSubscription {
    private AndesBridge mqqtServerChannel;


    public MQTTLocalSubscription(String subscriptionAsStr) {
        super(subscriptionAsStr);
        setTargetBoundExchange();
        setIsTopic();
        setNodeInfo();
        setIsActive(true);


    }

    public AndesBridge getMqqtServerChannel() {
        return mqqtServerChannel;
    }

    public void setMqqtServerChannel(AndesBridge mqqtServerChannel) {
        this.mqqtServerChannel = mqqtServerChannel;
    }

    /*Will include the MQTT topic name*/
    public void setTopic(String dest) {
        this.destination = dest;
    }

    /*Will inclue a subscription id*/
    public void setSubscriptionID(String id) {
        this.subscriptionID = id;
    }

    /*Will override the target bound exchange*/
    public void setTargetBoundExchange() {
        this.targetQueueBoundExchange = "MQQT";
    }

    /*Will set the topic stuff*/
    public void setIsTopic() {
        this.isBoundToTopic = true;
    }

    /*Will add the node information*/
    public void setNodeInfo() {
        this.subscribedNode = AndesUtils.getTopicNodeQueueName();
    }

    /*Provide the external subscriptions*/
    public void setIsActive(boolean isActive) {
        this.hasExternalSubscriptions = isActive;
    }

    @Override
    public int getnotAckedMsgCount() {
        return 0;
    }


    @Override
    public void sendMessageToSubscriber(AndesMessageMetadata messageMetadata) throws AndesException {
        //Should get the message from the list
        ByteBuffer message = MQTTUtils.getContentFromMetaInformation(messageMetadata);
        //Will publish the message to the respective queue
        if (mqqtServerChannel != null) {
            mqqtServerChannel.sendMessageToLocalProcessorForSubscription(messageMetadata.getDestination(), "MOST_ONE", message, false, messageMetadata.getMessageID());
        }
    }

    @Override
    public boolean isActive() {
        return true;
    }

    @Override
    public LocalSubscription createQueueToListentoTopic() {
        return null;
    }

    public boolean equals(Object o)
    {
        if (o instanceof MQTTLocalSubscription)
        {
            MQTTLocalSubscription c = (MQTTLocalSubscription) o;
            if ( this.subscriptionID.equals(c.subscriptionID) &&
                    this.getSubscribedNode().equals(c.getSubscribedNode()) &&
                    this.targetQueue.equals(c.targetQueue) &&
                    this.targetQueueBoundExchange.equals(c.targetQueueBoundExchange)) {
                return true;
            }
        }
        return false;
    }

    public int hashCode() {
        return new HashCodeBuilder(17, 31).
                append(subscriptionID).
                append(getSubscribedNode()).
                append(targetQueue).
                append(targetQueueBoundExchange).
                toHashCode();
    }
}
