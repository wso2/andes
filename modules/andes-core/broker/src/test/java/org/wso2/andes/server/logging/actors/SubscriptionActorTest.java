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
package org.wso2.andes.server.logging.actors;

import java.util.List;

import org.wso2.andes.server.subscription.MockSubscription;

/**
 * Test : AMQPConnectionActorTest
 * Validate the AMQPConnectionActor class.
 *
 * The test creates a new AMQPActor and then logs a message using it.
 *
 * The test then verifies that the logged message was the only one created and
 * that the message contains the required message.
 */
public class SubscriptionActorTest extends BaseConnectionActorTestCase
{

    @Override
    public void createBroker() throws Exception
    {
        super.createBroker();

        MockSubscription mockSubscription = new MockSubscription();

        mockSubscription.setQueue(getQueue(), false);

        _amqpActor = new SubscriptionActor(_rootLogger, mockSubscription);
    }

    /**
     * Test the AMQPActor logging as a Subscription logger.
     *
     * The test sends a message then verifies that it entered the logs.
     *
     * The log message should be fully repalaced (no '{n}' values) and should
     * contain subscription identification.
     */
    public void testSubscription()
    {
        final String message = sendTestLogMessage(_amqpActor);

        List<Object> logs = _rawLogger.getLogMessages();

        assertEquals("Message log size not as expected.", 1, logs.size());

        // Verify that the logged message is present in the output
        assertTrue("Message was not found in log message",
                   logs.get(0).toString().contains(message));

        // Verify that all the values were presented to the MessageFormatter
        // so we will not end up with '{n}' entries in the log.
        assertFalse("Verify that the string does not contain any '{'.",
                    logs.get(0).toString().contains("{"));

        // Verify that the message has the correct type
        assertTrue("Message contains the [sub: prefix",
                   logs.get(0).toString().contains("[sub:"));

        // Verify that the logged message does not contains the 'ch:' marker
        assertFalse("Message was logged with a channel identifier." + logs.get(0),
                    logs.get(0).toString().contains("/ch:"));
    }

}
