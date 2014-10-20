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
package org.wso2.andes.server.connection;

import org.apache.log4j.Logger;
import org.wso2.andes.AMQException;
import org.wso2.andes.common.Closeable;
import org.wso2.andes.protocol.AMQConstant;
import org.wso2.andes.server.protocol.AMQConnectionModel;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class ConnectionRegistry implements IConnectionRegistry, Closeable
{
    private List<AMQConnectionModel> _registry = new CopyOnWriteArrayList<AMQConnectionModel>();

    private Logger _logger = Logger.getLogger(ConnectionRegistry.class);

    public void initialise()
    {
        // None required
    }

    /** Close all of the currently open connections. */
    public void close()
    {
        while (!_registry.isEmpty())
        {
            AMQConnectionModel connection = _registry.get(0);
            closeConnection(connection, AMQConstant.INTERNAL_ERROR, "Broker is shutting down");
        }
    }
    
    public void closeConnection(AMQConnectionModel connection, AMQConstant cause, String message)
    {
        try
        {
            connection.close(cause, message);
        }
        catch (AMQException e)
        {
            _logger.warn("Error closing connection:" + e.getMessage());
        }
    }

    public void registerConnection(AMQConnectionModel connnection)
    {
        _registry.add(connnection);
    }

    public void deregisterConnection(AMQConnectionModel connnection)
    {
        _registry.remove(connnection);
    }

    public List<AMQConnectionModel> getConnections()
    {
        return new ArrayList<AMQConnectionModel>(_registry);
    }
}
