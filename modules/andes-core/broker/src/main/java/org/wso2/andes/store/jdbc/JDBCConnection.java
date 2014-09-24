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

package org.wso2.andes.store.jdbc;

import org.apache.log4j.Logger;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.DurableStoreConnection;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * JDBC connection class.
 * Connection is made using the jndi lookup name provided and connection pooled data source is
 * used to create new connections
 */
public class JDBCConnection implements DurableStoreConnection {

    private static final Logger logger = Logger.getLogger(JDBCConnection.class);
    private boolean isConnected;
    private DataSource datasource;

    @Override
    public void initialize(String jndiLookupName) throws AndesException {
        Connection connection = null;
        isConnected = false;
        try {
            datasource = InitialContext.doLookup(jndiLookupName);
            connection = datasource.getConnection();
            isConnected = true; // if no errors
        } catch (SQLException e) {
            throw new AndesException("Connecting to H2 database failed!", e);
        } catch (NamingException e) {
            throw new AndesException("Couldn't look up jndi entry for " +
                    "\"" + jndiLookupName + "\"" + e);
        } finally {
            close(connection, "Initialising database");
        }
    }

    /**
     * connection pooled data source object is returned.
     * Connections to database can be created using the data source.
     * @return DataSource
     */
    public DataSource getDataSource() {
        return datasource;
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
        return this;
    }

    /**
     * Closes the provided connection. on failure log the error;
     *
     * @param connection Connection
     */
    private void close(Connection connection, String task) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                logger.error("Failed to close connection after " + task);
            }
        }
    }
}
