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
package org.wso2.andes.server.security.auth.sasl;

import java.security.Principal;
import java.util.Set;

import javax.security.auth.Subject;

/** A principal that is just a wrapper for a simple username. */
public class UsernamePrincipal implements Principal
{
    private final String _name;

    public UsernamePrincipal(String name)
    {
        if (name == null)
        {
            throw new IllegalArgumentException("name cannot be null");
        }
        _name = name;
    }

    public String getName()
    {
        return _name;
    }

    public String toString()
    {
        return _name;
    }

    /**
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode()
    {
        final int prime = 31;
        return prime * _name.hashCode();
    }

    /**
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
        {
            return true;
        }
        else
        {
            if (obj instanceof UsernamePrincipal)
            {
                UsernamePrincipal other = (UsernamePrincipal) obj;
                return _name.equals(other._name);
            }
            else
            {
                return false;
            }
        }
    }
}
