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
package org.wso2.andes.server.logging;

import java.util.Arrays;
import java.util.List;

import org.wso2.andes.server.configuration.ServerConfiguration;
import org.wso2.andes.server.logging.subjects.AbstractTestLogSubject;
import org.wso2.andes.util.LogMonitor;

/**
 * The MessageStore test suite validates that the follow log messages as
 * specified in the Functional Specification.
 *
 * This suite of tests validate that the MessageStore messages occur correctly
 * and according to the following format:
 *
 * MST-1001 : Created : <name>
 * MST-1003 : Closed
 *
 * NOTE: Only for Persistent Stores
 * MST-1002 : Store location : <path>
 * MST-1004 : Recovery Start [: <queue.name>]
 * MST-1005 : Recovered <count> messages for queue <queue.name>
 * MST-1006 : Recovery Complete [: <queue.name>]
 */
public class MemoryMessageStoreLoggingTest extends AbstractTestLogging
{
    protected static final String MESSAGES_STORE_PREFIX = "MST-";

    public void setUp() throws Exception
    {
        //We explicitly do not call super.setUp as starting up the broker is
        //part of the test case.
        // So we have to make the new Log Monitor here

        _monitor = new LogMonitor(_outputFile);
    }

    /**
     * Description:
     * During Virtualhost startup a MessageStore will be created. The first MST
     * message that must be logged is the MST-1001 MessageStore creation.
     * Input:
     * Default configuration
     * Output:
     * <date> MST-1001 : Created : <name>
     *
     * Validation Steps:
     *
     * 1. The MST ID is correct
     * 2. The <name> is the correct MessageStore type as specified in the Default configuration
     *
     * @throws Exception caused by broker startup
     */
    public void testMessageStoreCreation() throws Exception
    {
        assertLoggingNotYetOccured(MESSAGES_STORE_PREFIX);

        super.setUp();

        List<String> results = waitAndFindMatches(MESSAGES_STORE_PREFIX);

        // Validation

        assertTrue("MST messages not logged", results.size() > 0);

        String log = getLogMessage(results, 0);
        //1
        assertEquals("MST-1001 is not the first MST message", "MST-1001", getMessageID(fromMessage(log)));

        //Validate each vhost logs a creation
        results = waitAndFindMatches("MST-1001");

        // Load VirtualHost list from file.
        List<String> vhosts = Arrays.asList(getServerConfig().getVirtualHosts());

        assertEquals("Each vhost did not create a store.", vhosts.size(), results.size());

        for (int index = 0; index < results.size(); index++)
        {
            String result = getLogMessage(results, index);

            // getSlice will return extract the vhost from vh(/test) -> '/test'
            // so remove the '/' to get the name
            String vhostName = AbstractTestLogSubject.getSlice("vh", result).substring(1);

            // Get the store class used in the configuration for the virtualhost.
            String fullStoreName = getServerConfig().getVirtualHostConfig(vhostName).getMessageStoreClass();

            // Get the Simple class name from the expected class name of o.a.q.s.s.MMS
            String storeName = fullStoreName.substring(fullStoreName.lastIndexOf(".") + 1);

            assertTrue("MST-1001 does not contains correct store name:"
                       + storeName + ":" + result, getMessageString(result).endsWith(storeName));

            assertEquals("The store name does not match expected value",
                         storeName, AbstractTestLogSubject.getSlice("ms", fromSubject(result)));
        }
    }

    /**
     * Description:
     * During shutdown the MessageStore will also cleanly close. When this has
     * completed a MST-1003 closed message will be logged. No further messages
     * from this MessageStore will be logged after this message.
     *
     * Input:
     * Default configuration
     * Output:
     * <date> MST-1003 : Closed
     *
     * Validation Steps:
     *
     * 1. The MST ID is correct
     * 2. This is teh last log message from this MessageStore
     *
     * @throws Exception caused by broker startup
     */
    public void testMessageStoreClose() throws Exception
    {
        assertLoggingNotYetOccured(MESSAGES_STORE_PREFIX);

        super.setUp();

        //Stop the broker so we get the close messages.
        stopBroker();

        List<String> results = waitAndFindMatches(MESSAGES_STORE_PREFIX);

        // Validation

        assertTrue("MST messages not logged", results.size() > 0);

        // Load VirtualHost list from file.
        ServerConfiguration configuration = new ServerConfiguration(_configFile);
        configuration.initialise();
        List<String> vhosts = Arrays.asList(configuration.getVirtualHosts());

        //Validate each vhost logs a creation
        results = waitAndFindMatches("MST-1003");

        assertEquals("Each vhost did not close its store.", vhosts.size(), results.size());

        for (int index = 0; index < results.size(); index++)
        {
            String result = getLogMessage(results, index);

            // getSlice will return extract the vhost from vh(/test) -> '/test'
            // so remove the '/' to get the name
            String vhostName = AbstractTestLogSubject.getSlice("vh", result).substring(1);

            // Get the store class used in the configuration for the virtualhost.
            String fullStoreName = configuration.getVirtualHostConfig(vhostName).getMessageStoreClass();

            // Get the Simple class name from the expected class name of o.a.q.s.s.MMS
            String storeName = fullStoreName.substring(fullStoreName.lastIndexOf(".") + 1);

            assertEquals("MST-1003 does not close:",
                         "Closed", getMessageString(result));

            assertEquals("The store name does not match expected value",
                         storeName, AbstractTestLogSubject.getSlice("ms", fromSubject(result)));
        }
    }

}
