/*
 * Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.andes.server.security;

import org.wso2.andes.server.security.auth.database.PrincipalDatabase;
import org.wso2.andes.server.security.auth.sasl.AuthenticationProviderInitialiser;
import org.wso2.andes.server.security.auth.sasl.UsernamePrincipal;
import org.wso2.andes.server.security.auth.sasl.plain.PlainInitialiser;

import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.login.AccountNotFoundException;
import java.io.IOException;
import java.security.Principal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Carbon-based principal database for Apache Qpid. This uses Carbon user manager to handle authentication
 */
public class CarbonBasedPrincipalDatabase implements PrincipalDatabase {

    private Map<String, AuthenticationProviderInitialiser> saslServers;

    public CarbonBasedPrincipalDatabase() {
        saslServers = new HashMap<String, AuthenticationProviderInitialiser>();

        // Accept Plain incoming and compare it with UM value
        PlainInitialiser plain = new PlainInitialiser();
        plain.initialise(this);

        saslServers.put(plain.getMechanismName(), plain);
    }

    /**
     * Get list of SASL mechanism objects. We only use PLAIN.
     *
     * @return List of mechanism objects
     */
    public Map<String, AuthenticationProviderInitialiser> getMechanisms() {
        return saslServers;
    }

    public List<Principal> getUsers() {
        return null;
    }

    public boolean deletePrincipal(Principal principal)
            throws AccountNotFoundException {
        return true;
    }

    /**
     * Create Principal instance for a valid user
     *
     * @param username Principal username
     * @return Principal instance
     */
    public Principal getUser(String username) {
        return new UsernamePrincipal(username);
    }

    public boolean verifyPassword(String principal, char[] password)
            throws AccountNotFoundException {
        return true;
    }

    public boolean updatePassword(Principal principal, char[] password)
            throws AccountNotFoundException {
        return true;
    }

    public boolean createPrincipal(Principal principal, char[] password) {
        return true;
    }

    public void reload() throws IOException {
    }

    /**
     * This method sets of a given principal is authenticated or not.
     *
     * @param principal        Principal to be authenticated
     * @param passwordCallback Callback to set if the user is authenticated or not. This also holds user's password.
     * @throws IOException
     * @throws AccountNotFoundException
     */
    public void setPassword(Principal principal, PasswordCallback passwordCallback)
            throws IOException, AccountNotFoundException {
    }

}
