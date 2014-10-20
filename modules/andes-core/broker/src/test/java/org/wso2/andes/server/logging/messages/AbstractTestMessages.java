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
package org.wso2.andes.server.logging.messages;

import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.wso2.andes.server.logging.LogActor;
import org.wso2.andes.server.logging.LogMessage;
import org.wso2.andes.server.logging.LogSubject;
import org.wso2.andes.server.logging.UnitTestMessageLogger;
import org.wso2.andes.server.logging.actors.TestLogActor;
import org.wso2.andes.server.logging.subjects.TestBlankSubject;
import org.wso2.andes.server.util.InternalBrokerBaseCase;

public abstract class AbstractTestMessages extends InternalBrokerBaseCase
{
    protected Configuration _config = new PropertiesConfiguration();
    protected LogMessage _logMessage = null;
    protected LogActor _actor;
    protected UnitTestMessageLogger _logger;
    protected LogSubject _logSubject = new TestBlankSubject();

    @Override
    public void setUp() throws Exception
    {
        super.setUp();

        _logger = new UnitTestMessageLogger();

        _actor = new TestLogActor(_logger);
    }

    protected List<Object> performLog()
    {
        if (_logMessage == null)
        {
            throw new NullPointerException("LogMessage has not been set");
        }

        _actor.message(_logSubject, _logMessage);

        return _logger.getLogMessages();
    }

    /**
     * Validate that only a single log messasge occured and that the message
     * section starts with the specified tag
     *
     * @param logs     the logs generated during test run
     * @param tag      the tag to check for
     * @param expected the expected log messages
     *
     */
    protected void validateLogMessage(List<Object> logs, String tag, String[] expected)
    {
        assertEquals("Log has incorrect message count", 1, logs.size());

        //We trim() here as we don't care about extra white space at the end of the log message
        // but we do care about the ability to easily check we don't have unexpected text at
        // the end.
        String log = String.valueOf(logs.get(0)).trim();

        // Simple switch to print out all the logged messages 
        //System.err.println(log);

        int msgIndex = log.indexOf(_logSubject.toLogString())+_logSubject.toLogString().length();

        assertTrue("Unable to locate Subject:" + log, msgIndex != -1);

        String message = log.substring(msgIndex);

        assertTrue("Message does not start with tag:" + tag + ":" + message,
                   message.startsWith(tag));

        // Test that the expected items occur in order.
        int index = 0;
        for (String text : expected)
        {
            index = message.indexOf(text, index);
            assertTrue("Message does not contain expected (" + text + ") text :" + message, index != -1);
            index = index + text.length();
        }

        //Check there is nothing left on the log message
        assertEquals("Message has more text. '" + log.substring(msgIndex + index) + "'",
                     log.length(), msgIndex +  index);
    }

}
