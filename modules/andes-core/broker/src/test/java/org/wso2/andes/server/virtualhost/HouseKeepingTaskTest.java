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
package org.wso2.andes.server.virtualhost;

import java.util.concurrent.CountDownLatch;

import org.wso2.andes.server.logging.LogActor;
import org.wso2.andes.server.logging.NullRootMessageLogger;
import org.wso2.andes.server.logging.actors.CurrentActor;
import org.wso2.andes.server.logging.actors.TestLogActor;
import org.wso2.andes.test.utils.QpidTestCase;

public class HouseKeepingTaskTest extends QpidTestCase
{
    /**
     * Tests that the abstract HouseKeepingTask properly cleans up any LogActor
     * it adds to the CurrentActor stack by verifying the CurrentActor set
     * before task execution is the CurrentActor after execution.
     */
    public void testCurrentActorStackBalance() throws Exception
    {
        //create and set a test actor
        LogActor testActor = new TestLogActor(new NullRootMessageLogger());
        CurrentActor.set(testActor);

        //verify it is returned correctly before executing a HouseKeepingTask
        assertEquals("Expected LogActor was not returned", testActor, CurrentActor.get());

        final CountDownLatch latch = new CountDownLatch(1);
        HouseKeepingTask testTask = new HouseKeepingTask(new MockVirtualHost("HouseKeepingTaskTestVhost"))
        {
            @Override
            public void execute()
            {
                latch.countDown();
            }
        };

        //run the test HouseKeepingTask using the current Thread to influence its CurrentActor stack
        testTask.run();

        assertEquals("The expected LogActor was not returned, the CurrentActor stack has become unbalanced",
                     testActor, CurrentActor.get());
        assertEquals("HouseKeepingTask execute() method was not run", 0, latch.getCount());

        //clean up the test actor
        CurrentActor.remove();
    }
}
