/*
*  Copyright (c) 2005-2010, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
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

/**
 * All the subscriptions relation to a topic will be maintained though the following class, attributes such as QOS
 * levels will be maintained here
 */
public class MQTTSubscriber {
    //TODO QOS level information should be accessed for use cases which will be implimented in future
    //The level of QOS the subscriber is bound to
    private int QOS_Level;
    //Specifies whether the subscription is durable or not
    private boolean isCleanSession;
    //Specifies the channel id of the subscriber
    private String subscriberChannelID;

    /**
     * Will allow retrival of the unique identifyer of the subscriber
     *
     * @return the identifier of the subscriber
     */
    public String getSubscriberChannelID() {
        return subscriberChannelID;
    }

    /**
     * Set the id generated for the subscriber locally
     * @param subscriberChannelID the unique subscription identifier
     */
    public void setSubscriberChannelID(String subscriberChannelID) {
        this.subscriberChannelID = subscriberChannelID;
    }

    /**
     * Indicates whether the subscription is durable or not, false if not
     *
     * @param isCleanSession whether the subscription is durable
     */
    public void setCleanSession(boolean isCleanSession) {
        this.isCleanSession = isCleanSession;
    }

    /**
     * Will set the lvel of QOS the subscriber is bound to
     *
     * @param QOS_Level the QOS level, this can either be 1,2 or 3
     */
    public void setQOS_Level(int QOS_Level) {
        this.QOS_Level = QOS_Level;
    }

}
