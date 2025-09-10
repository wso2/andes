/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.wso2.andes.systest;

import org.apache.commons.configuration2.ex.ConfigurationException;
import org.wso2.andes.AMQChannelClosedException;
import org.wso2.andes.AMQException;
import org.wso2.andes.client.AMQSession_0_10;
import org.wso2.andes.jms.ConnectionListener;
import org.wso2.andes.protocol.AMQConstant;

import javax.jms.Session;
import javax.naming.NamingException;
import java.io.IOException;

public class MergeConfigurationTest extends TestingBaseCase
{

    protected int topicCount = 0;


    public void configureTopic(String topic, int msgCount) throws NamingException, IOException, ConfigurationException
    {

        setProperty(".topics.topic("+topicCount+").name", topic);
        setProperty(".topics.topic("+topicCount+").slow-consumer-detection.messageCount", String.valueOf(msgCount));
        setProperty(".topics.topic("+topicCount+").slow-consumer-detection.policy.name", "TopicDelete");
        topicCount++;
    }


    /**
     * Test that setting messageCount takes affect on topics
     *
     * We send 10 messages and disconnect at 9
     *
     * @throws Exception
     */
    public void testTopicConsumerMessageCount() throws Exception
    {
        MAX_QUEUE_MESSAGE_COUNT = 10;

        configureTopic(getName(), (MAX_QUEUE_MESSAGE_COUNT * 4) - 1);

        //Configure topic as a subscription
        setProperty(".topics.topic("+topicCount+").subscriptionName", "clientid:"+getTestQueueName());
        configureTopic(getName(), (MAX_QUEUE_MESSAGE_COUNT - 1));



        //Start the broker
        startBroker();

        topicConsumer(Session.AUTO_ACKNOWLEDGE, true);
    }

}
