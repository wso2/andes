/*
  * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
  *
  *   WSO2 Inc. licenses this file to you under the Apache License,
  *   Version 2.0 (the "License"); you may not use this file except
  *   in compliance with the License.
  *   You may obtain a copy of the License at
  *
  *      http://www.apache.org/licenses/LICENSE-2.0
  *
  *   Unless required by applicable law or agreed to in writing,
  *   software distributed under the License is distributed on an
  *   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  *   KIND, either express or implied.  See the License for the
  *   specific language governing permissions and limitations
  *   under the License.
  */

package org.wso2.andes.amqp;

import org.wso2.andes.AMQProtocolException;
import org.wso2.carbon.andes.core.internal.AndesContext;
import org.wso2.carbon.andes.core.security.AndesAuthenticationManager;
import org.wso2.andes.security.UsernamePasswordCallbackHandler;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginException;
import java.io.UnsupportedEncodingException;
import java.security.Principal;

/**
 * Manages authentication of QPID objects against Andes.
 */
public class AMQPAuthenticationManager {

    /**
     * The separator used to separate username and password in the encoded QPID response
     */
    public static final byte CREDENTIALS_SEPARATOR = (byte) 0;

    /**
     * The charset used to encode QPID credentials response.
     */
    public static final String RESPONSE_CHARSET = "UTF8";

    private static AndesAuthenticationManager andesAuthenticationManager =
            AndesContext.getInstance().getAndesAuthenticationManager();

    /**
     * Authenticate a QPID encoded response against Andes. Throws {@link LoginException} if not authenticated.
     *
     * @param response The encoded credentials response from QPID
     * @return The authenticated subject if authenticated
     * @throws LoginException
     */
    public static Subject authenticate(byte[] response) throws LoginException {
        CallbackHandler callbackHandler = null;
        try {
            callbackHandler = extractCredentialsFromResponse(response);
        } catch (AMQProtocolException e) {
            throw new LoginException("Error decoding the credentials : " +  e.getLocalizedMessage());
        }

        return andesAuthenticationManager.authenticate(callbackHandler);
    }

    /**
     * Extract credentials information from the AMQP clients response.
     *
     * @param response The response byte array
     * @return CallbackHandler initialized with extract credentials
     * @throws AMQProtocolException
     * @throws UnsupportedEncodingException
     */
    private static CallbackHandler extractCredentialsFromResponse(byte[] response) throws AMQProtocolException {

        int usernameStartPosition = findNullPosition(response, 0);
        if (usernameStartPosition < 0) {
            throw new AMQProtocolException(null, "Invalid encoding, username start position not found", null);
        }
        int passwordStartPosition = findNullPosition(response, usernameStartPosition + 1);
        if (passwordStartPosition < 0) {
            throw new AMQProtocolException(null, "Invalid encoding, password start position not found", null);
        }

        String username = null;
        String password = null;
        try {
            username = new String(response, usernameStartPosition +  1, passwordStartPosition -
                                                                      usernameStartPosition - 1, RESPONSE_CHARSET);

            int passwordLen = response.length - passwordStartPosition - 1;
            password = new String(response, passwordStartPosition + 1, passwordLen, RESPONSE_CHARSET);
        } catch (UnsupportedEncodingException e) {
            throw new AMQProtocolException(null, "Invalid encoding found in credentials", e );
        }

        UsernamePasswordCallbackHandler callbackHandler = new UsernamePasswordCallbackHandler();
        callbackHandler.initialise(username, password);

        return callbackHandler;
    }

    /**
     * Find the position of character ((byte) 0) in a byte array.
     *
     * @param bytes The data byte array to find the null character
     * @param startPosition The position to start seeking from
     *
     * @return The position the null character is found. Returns -1 if not found.
     */
    private static int findNullPosition(byte[] bytes, int startPosition) {
        int position = startPosition;
        while (position < bytes.length) {
            if (bytes[position] == CREDENTIALS_SEPARATOR) {
                return position;
            }
            position++;
        }
        return -1;
    }

    public static Principal extractUserPrincipalFromSubject(Subject subject) {
        return andesAuthenticationManager.extractUserPrincipalFromSubject(subject);
    }
}