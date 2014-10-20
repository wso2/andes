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
import org.wso2.andes.server.logging.LogSubject;
import org.wso2.andes.server.logging.RootMessageLogger;

public class GenericActor extends AbstractActor
{

    private static RootMessageLogger _defaultMessageLogger;

    private LogSubject _logSubject;

    public static RootMessageLogger getDefaultMessageLogger()
    {
        return _defaultMessageLogger;
    }

    public static void setDefaultMessageLogger(RootMessageLogger defaultMessageLogger)
    {
        _defaultMessageLogger = defaultMessageLogger;
    }

    public GenericActor(LogSubject logSubject, RootMessageLogger rootLogger)
    {
        super(rootLogger);
        _logSubject = logSubject;
    }

    public String getLogMessage()
    {
        return _logSubject.toLogString();
    }

    public static LogActor getInstance(final String logMessage, RootMessageLogger rootLogger)
    {
        return new GenericActor(new LogSubject()
        {
            public String toLogString()
            {
                return logMessage;
            }

        }, rootLogger);
    }

    public static LogActor getInstance(final String subjectMessage)
    {
        return new GenericActor(new LogSubject()
        {
            public String toLogString()
            {
                return "[" + subjectMessage + "] ";
            }

        }, _defaultMessageLogger);
    }

    public static LogActor getInstance(LogSubject logSubject)
    {
        return new GenericActor(logSubject, _defaultMessageLogger);
    }
}
