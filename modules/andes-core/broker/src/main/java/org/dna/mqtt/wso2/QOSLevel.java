/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.dna.mqtt.wso2;

/**
 * Defines the three levels of QoS the message delivery/distribution could be at
 */
public enum QOSLevel {

    /**
     * The message will be delivered/distributed at its best performance effort, the level of QoS would be 0
     * Note: There's a possibility of the message not getting delivered/distributed to its interested parties
     */
    AT_MOST_ONCE(0),
    /**
     * The message would be delivered at least once to the subscription, the level of QoS would be 1
     * Note: Message duplication could occur when using this QoS
     */
    AT_LEAST_ONCE(1),
    /**
     * The message will be delivered to its best reliable efforts ensuring exactly once delivery,
     * the level of QoS would be 2
     * Note: This level of QoS the performance would be less in comparison to the other two.
     */
    EXACTLY_ONCE(2);

    //Will hold the level of QoS i.e 0,1 or 2
    private int qosValue;

    private QOSLevel(int value) {
        this.qosValue = value;
    }

    public int getValue() {
        return qosValue;
    }

    /**
     * Returns the corresponding enum from its value
     *
     * @param qos the level of QoS
     * @return the level of QoS as its enum representation
     */
    public static QOSLevel getQoSFromValue(int qos) {
        switch (qos) {
            case 0:
                return QOSLevel.AT_MOST_ONCE;
            case 1:
                return QOSLevel.AT_LEAST_ONCE;
            case 2:
                return QOSLevel.EXACTLY_ONCE;
            default:
                return QOSLevel.AT_MOST_ONCE;
        }
    }

}
