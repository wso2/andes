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
package org.wso2.andes.client.security;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;

import junit.framework.TestCase;

import org.wso2.andes.client.AMQConnectionURL;
import org.wso2.andes.client.MockAMQConnection;

/**
 * Unit tests for the UsernamePasswordCallbackHandler.
 *
 */
public class UsernamePasswordCallbackHandlerTest extends TestCase
{
    private AMQCallbackHandler _callbackHandler = new UsernamePasswordCallbackHandler(); // Class under test
    private static final String PROMPT_UNUSED = "unused";

    @Override
    protected void setUp() throws Exception
    {
        super.setUp();

        final String url = "amqp://username:password@client/test?brokerlist='tcp://localhost:1'";

        _callbackHandler.initialise(new AMQConnectionURL(url));
    }

    /**
     *  Tests that the callback handler can correctly retrieve the username from the connection url.
     */
    public void testNameCallback() throws Exception
    {
        final String expectedName = "username";
        NameCallback nameCallback = new NameCallback(PROMPT_UNUSED);

        assertNull("Unexpected name before test", nameCallback.getName());
        _callbackHandler.handle(new Callback[] {nameCallback});
        assertEquals("Unexpected name", expectedName, nameCallback.getName());
    }

    /**
     *  Tests that the callback handler can correctly retrieve the password from the connection url.
     */
    public void testPasswordCallback() throws Exception
    {
        final String expectedPassword = "password";
        PasswordCallback passwordCallback = new PasswordCallback(PROMPT_UNUSED, false);
        assertNull("Unexpected password before test", passwordCallback.getPassword());
        _callbackHandler.handle(new Callback[] {passwordCallback});
        assertEquals("Unexpected password", expectedPassword, new String(passwordCallback.getPassword()));
    }    
}
