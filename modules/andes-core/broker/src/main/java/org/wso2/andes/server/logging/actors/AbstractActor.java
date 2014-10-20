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
package org.wso2.andes.server.logging.actors;

import org.wso2.andes.server.logging.LogActor;
import org.wso2.andes.server.logging.LogMessage;
import org.wso2.andes.server.logging.LogSubject;
import org.wso2.andes.server.logging.RootMessageLogger;

public abstract class AbstractActor implements LogActor
{
    public final String _msgPrefix = System.getProperty("qpid.logging.prefix","");

    protected RootMessageLogger _rootLogger;

    public AbstractActor(RootMessageLogger rootLogger)
    {
        if(rootLogger == null)
        {
            throw new NullPointerException("RootMessageLogger cannot be null");
        }
        _rootLogger = rootLogger;
    }

    public void message(LogSubject subject, LogMessage message)
    {
        if (_rootLogger.isMessageEnabled(this, subject, message.getLogHierarchy()))
        {
            _rootLogger.rawMessage(_msgPrefix + getLogMessage() + subject.toLogString() + message, message.getLogHierarchy());
        }
    }

    public void message(LogMessage message)
    {
        if (_rootLogger.isMessageEnabled(this, message.getLogHierarchy()))
        {
            _rootLogger.rawMessage(_msgPrefix + getLogMessage() + message, message.getLogHierarchy());
        }
    }

    public RootMessageLogger getRootMessageLogger()
    {
        return _rootLogger;
    }

    public String toString()
    {
        return getLogMessage();
    }

    abstract public String getLogMessage();

}
