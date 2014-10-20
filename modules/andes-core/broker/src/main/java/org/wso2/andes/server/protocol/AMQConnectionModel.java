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
package org.wso2.andes.server.protocol;

import java.util.List;
import java.util.UUID;

import org.wso2.andes.AMQException;
import org.wso2.andes.protocol.AMQConstant;
import org.wso2.andes.server.logging.LogSubject;
import org.wso2.andes.server.stats.StatisticsGatherer;

public interface AMQConnectionModel extends StatisticsGatherer
{
    /**
     * get a unique id for this connection.
     * 
     * @return a {@link UUID} representing the connection
     */
    public UUID getId();
    
    /**
     * Close the underlying Connection
     * 
     * @param cause
     * @param message
     * @throws org.wso2.andes.AMQException
     */
    public void close(AMQConstant cause, String message) throws AMQException;

    /**
     * Close the given requested Session
     * 
     * @param session
     * @param cause
     * @param message
     * @throws org.wso2.andes.AMQException
     */
    public void closeSession(AMQSessionModel session, AMQConstant cause, String message) throws AMQException;

    public long getConnectionId();
    
    /**
     * Get a list of all sessions using this connection.
     * 
     * @return a list of {@link AMQSessionModel}s
     */
    public List<AMQSessionModel> getSessionModels();

    /**
     * Return a {@link LogSubject} for the connection.
     */
    public LogSubject getLogSubject();
}
