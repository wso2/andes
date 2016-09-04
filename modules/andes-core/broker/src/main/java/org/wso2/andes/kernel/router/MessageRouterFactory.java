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

package org.wso2.andes.kernel.router;

import org.wso2.andes.amqp.AMQPUtils;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.mqtt.utils.MQTTUtils;

/**
 * Factory for creating message routers
 */
public class MessageRouterFactory {


    /**
     * Create AndesMessageRouter with information given. This will generate an object with specific behaviour
     * according to message router name
     *
     * @param messageRouterName name of the message router
     * @param type              type of the router
     * @param autoDelete        true if message router is to be deleted when all queues detached
     * @return AndesMessageRouter impl
     * @throws AndesException
     */
    public AndesMessageRouter createMessageRouter(String messageRouterName, String type, boolean autoDelete)
            throws AndesException {
        AndesMessageRouter messageRouter;
        if (messageRouterName.equals(AMQPUtils.DEFAULT_EXCHANGE_NAME)) {
            messageRouter = new QueueMessageRouter(messageRouterName, type, autoDelete);
        } else if (messageRouterName.equals(AMQPUtils.DIRECT_EXCHANGE_NAME)) {
            messageRouter = new QueueMessageRouter(messageRouterName, type, autoDelete);
        } else if (messageRouterName.equals(AMQPUtils.TOPIC_EXCHANGE_NAME)) {
            messageRouter = new TopicMessageRouter(messageRouterName, type, autoDelete);
        } else if (messageRouterName.equals(MQTTUtils.MQTT_EXCHANGE_NAME)) {
            messageRouter = new MQTTMessageRouter(messageRouterName, type, autoDelete);
        } else if (messageRouterName.equals(AMQPUtils.DLC_EXCHANGE_NAME)) {
            messageRouter = new DiscardMessageRouter(messageRouterName, type, autoDelete);
        } else {    //default one will be a queue exchange
            messageRouter = new QueueMessageRouter(messageRouterName, type, autoDelete);
        }
        return messageRouter;
    }

    /**
     * Create AndesMessageRouter with encoded information. According to name of encoded string,
     * factory will generate the specific message router.
     *
     * @param messageRouterInfo encoded message router information
     * @return AndesMessageRouter impl
     * @throws AndesException
     */
    public AndesMessageRouter createMessageRouter(String messageRouterInfo)
            throws AndesException {
        AndesMessageRouter mockRouter = new QueueMessageRouter(messageRouterInfo);
        String messageRouterName = mockRouter.getName();
        AndesMessageRouter messageRouter = null;
        if (messageRouterName.equals(AMQPUtils.DEFAULT_EXCHANGE_NAME)) {
            messageRouter = new QueueMessageRouter(messageRouterInfo);
        } else if (messageRouterName.equals(AMQPUtils.DIRECT_EXCHANGE_NAME)) {
            messageRouter = new QueueMessageRouter(messageRouterInfo);
        } else if (messageRouterName.equals(AMQPUtils.TOPIC_EXCHANGE_NAME)) {
            messageRouter = new TopicMessageRouter(messageRouterInfo);
        } else if (messageRouterName.equals(MQTTUtils.MQTT_EXCHANGE_NAME)) {
            messageRouter = new MQTTMessageRouter(messageRouterInfo);
        } else if (messageRouterName.equals(AMQPUtils.DLC_EXCHANGE_NAME)) {
            messageRouter = new DiscardMessageRouter(messageRouterInfo);
        } else {    //default one will be a queue exchange
            messageRouter = new QueueMessageRouter(messageRouterInfo);
        }
        return messageRouter;
    }
}
