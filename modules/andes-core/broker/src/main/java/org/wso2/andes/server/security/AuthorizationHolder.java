/*
 * Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.andes.server.security;

import java.security.Principal;

import javax.security.auth.Subject;

import org.wso2.andes.server.security.auth.sasl.GroupPrincipal;
import org.wso2.andes.server.security.auth.sasl.UsernamePrincipal;

/**
 * Represents the authorization of the logged on user. 
 * 
 */
public interface AuthorizationHolder
{
    /** 
     * Returns the {@link Subject} of the authorized user.  This is guaranteed to
     * contain at least one {@link UsernamePrincipal}, representing the the identity
     * used when the user logged on to the application, and zero or more {@link GroupPrincipal}
     * representing the group(s) to which the user belongs.
     *
     * @return the Subject
     */
    Subject getAuthorizedSubject();

    /** 
     * Returns the {@link Principal} representing the the identity
     * used when the user logged on to the application.
     * 
     * @return a Principal
     */
    Principal getAuthorizedPrincipal();
}
