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
package org.wso2.andes.server.logging;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.wso2.andes.server.configuration.ServerConfiguration;

public class Log4jMessageLogger extends AbstractRootMessageLogger
{
    public static final Level LEVEL = Level.toLevel("INFO");
    
    public Log4jMessageLogger()
    {
        super();
    }

    public Log4jMessageLogger(ServerConfiguration config)
    {
        super(config);
    }
    
    @Override
    public boolean isMessageEnabled(LogActor actor, LogSubject subject, String logHierarchy)
    {
        return isMessageEnabled(actor, logHierarchy);
    }

    @Override
    public boolean isMessageEnabled(LogActor actor, String logHierarchy)
    {
        if(isEnabled())
        {
            Logger logger = Logger.getLogger(logHierarchy);
            return logger.isEnabledFor(LEVEL);
        }
        else
        {
            return false;
        }
    }

    @Override
    public void rawMessage(String message, String logHierarchy)
    {
        rawMessage(message, null, logHierarchy);
    }

    @Override
    public void rawMessage(String message, Throwable throwable, String logHierarchy)
    {
        Logger logger = Logger.getLogger(logHierarchy);
        
        logger.log(LEVEL, message, throwable);
    }
}
