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
package org.wso2.andes.server.queue;

import org.wso2.andes.framing.AMQShortString;
import org.wso2.andes.pool.ReferenceCountingExecutorService;
import org.wso2.andes.server.util.InternalBrokerBaseCase;
import org.wso2.andes.server.virtualhost.VirtualHost;

import org.wso2.andes.server.registry.ApplicationRegistry;
import org.wso2.andes.AMQException;

public class SimpleAMQQueueThreadPoolTest extends InternalBrokerBaseCase
{

    public void test() throws AMQException
    {
        int initialCount = ReferenceCountingExecutorService.getInstance().getReferenceCount();
        VirtualHost test = ApplicationRegistry.getInstance().getVirtualHostRegistry().getVirtualHost("test");

        try
        {
            SimpleAMQQueue queue = (SimpleAMQQueue) AMQQueueFactory.createAMQQueueImpl(new AMQShortString("test"), false,
                                                                                       new AMQShortString("owner"),
                                                                                       false, false, test, null);

            assertFalse("Creation did not start Pool.", ReferenceCountingExecutorService.getInstance().getPool().isShutdown());

            assertEquals("References not increased", initialCount + 1, ReferenceCountingExecutorService.getInstance().getReferenceCount());

            queue.stop();

            assertEquals("References not decreased", initialCount , ReferenceCountingExecutorService.getInstance().getReferenceCount());
        }
        finally
        {
            ApplicationRegistry.remove();
        }
    }
}
