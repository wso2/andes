/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.    
 *
 * 
 */
package org.wso2.andes.server.logging;

import java.util.LinkedList;
import java.util.List;

import org.wso2.andes.server.configuration.ServerConfiguration;

public class UnitTestMessageLogger extends AbstractRootMessageLogger
{
    private final List<Object> _log = new LinkedList<Object>();
    
    public UnitTestMessageLogger()
    {

    }

    public UnitTestMessageLogger(ServerConfiguration config)
    {
        super(config);
    }

    public void rawMessage(String message, String logHierarchy)
    {
        _log.add(message);
    }

    public void rawMessage(String message, Throwable throwable, String logHierarchy)
    {
        _log.add(message);

        if(throwable != null)
        {
            _log.add(throwable);
        }
    }


    public List<Object> getLogMessages()
    {
        return _log;
    }

    public void clearLogMessages()
    {
        _log.clear();
    }
    
    public boolean messageContains(final int index, final String contains)
    {
        if (index + 1 > _log.size())
        {
            throw new IllegalArgumentException("Message with index " + index + " has not been logged");
        }
        final String message = _log.get(index).toString();
        return message.contains(contains);
    }
}
