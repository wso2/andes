/*
*  Copyright (c) 2005-2010, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
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

package org.wso2.andes.messageStore;

import org.apache.commons.configuration.Configuration;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.DurableStoreConnection;

// todo: need to implement with reading cassandra data source from master-datasources.xml
public class H2Connection implements DurableStoreConnection {

    boolean isConnected;

    @Override
    public void initialize(Configuration configuration) throws AndesException {
        isConnected = true;
    }

    @Override
    public void close() {
        isConnected = false;
    }

    @Override
    public boolean isLive() {
        return isConnected;
    }

    @Override
    public Object getConnection() {
        return this; //
    }

    void setIsConnected(boolean isConnected) {
        this.isConnected = isConnected;
    }
}
