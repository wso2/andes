/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.andes.store.rdbms;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.wso2.andes.store.AndesBatchUpdateException;


/**
 * Contains utilility methods required for both {@link RDBMSMessageStoreImpl}
 * and {@link RDBMSAndesContextStoreImpl}
 */
public class RDBMSStoreUtils {

    private static final Logger log = Logger.getLogger(RDBMSStoreUtils.class);
    
    /**
     * Inserts a test record
     * 
     * @param testString
     *            a string value
     * @param testTime
     *            a time value
     * @return true if the test was successful.
     */
    public boolean testInsert(Connection connection, String testString, long testTime) throws SQLException {

        boolean canInsert = false;
        PreparedStatement preparedStatement = null;

        try {
            connection.setAutoCommit(false);

            preparedStatement = connection.prepareStatement(RDBMSConstants.PS_TEST_MSG_STORE_INSERT);

            preparedStatement.setString(1, testString);
            preparedStatement.setLong(2, testTime);

            preparedStatement.executeUpdate();
            connection.commit();
            canInsert = true;
        } catch (SQLException e) {
            rollback(connection,
                     RDBMSConstants.TASK_TEST_MESSAGE_STORE_OPERATIONAL_INSERT +
                             String.format("test data : [%s, %d]", testString, testTime));

        } finally {
            String msg =
                         RDBMSConstants.TASK_TEST_MESSAGE_STORE_OPERATIONAL_INSERT +
                                 String.format("test data : [%s, %d]", testString, testTime);
            close(preparedStatement, msg);
            close(connection, msg);
        }

        return canInsert;
    }
    
    /**
     * Reads a test record 
     * @param testString a string value
     * @param testTime a time value 
     * @return true if the test was successful. 
     */
    public boolean testRead(Connection connection, 
                            String testString, 
                            long testTime) throws SQLException{

        boolean canRead = false;
        
        PreparedStatement selectPreparedStatement = null;

        ResultSet results = null;

        try {
            selectPreparedStatement = connection.prepareStatement(
                                       RDBMSConstants.PS_TEST_MSG_STORE_SELECT);
            selectPreparedStatement.setString(1, testString);
            selectPreparedStatement.setLong(2, testTime);

            results = selectPreparedStatement.executeQuery();

            if (results.next()) {
                canRead = true;
            }

        } catch (SQLException e) {
            log.error(RDBMSConstants.TASK_TEST_MESSAGE_STORE_OPERATIONAL_READ +
                              String.format("test data : [%s, %d]", testString, testTime), e);

        } finally {
            String msg =
                         RDBMSConstants.TASK_TEST_MESSAGE_STORE_OPERATIONAL_READ +
                                 String.format("test data : [%s, %d]", testString, testTime);

            close(results, msg);
            close(selectPreparedStatement, msg);
            close(connection, msg);
        }

        return canRead;
    }
  
    /**
     * Delete a test record
     * 
     * @param testString
     *            a string value
     * @param testTime
     *            a time value
     * @return true if the test was successful.
     */
    public boolean testDelete(Connection connection, String testString, long testTime) throws SQLException {

        boolean canDelete = false;
        PreparedStatement preparedStatement = null;

        ResultSet results = null;

        try {
            connection.setAutoCommit(false);

            preparedStatement = connection.prepareStatement(RDBMSConstants.PS_TEST_MSG_STORE_DELETE);
            preparedStatement.setString(1, testString);
            preparedStatement.setLong(2, testTime);
            preparedStatement.executeUpdate();
            connection.commit();
            canDelete = true;
        } catch (SQLException e) {
            rollback(connection,
                     RDBMSConstants.TASK_TEST_MESSAGE_STORE_OPERATIONAL_DELETE +
                             String.format("test data : [%s, %d]", testString, testTime));
        } finally {

            String msg =
                         RDBMSConstants.TASK_TEST_MESSAGE_STORE_OPERATIONAL_DELETE +
                                 String.format("test data : [%s, %d]", testString, testTime);
            close(results, msg);
            close(preparedStatement, msg);
            close(connection, msg);
        }

        return canDelete;
    }
    
    
    /**
     * close the prepared statement resource
     *
     * @param preparedStatement PreparedStatement
     * @param task              task that was done by the closed prepared statement.
     */
    private void close(PreparedStatement preparedStatement, String task) {
        if (preparedStatement != null) {
            try {
                preparedStatement.close();
            } catch (SQLException e) {
                log.error("Closing prepared statement failed after " + task, e);
            }
        }
    }

    /**
     * closes the result set resources
     *
     * @param resultSet ResultSet
     * @param task      task that was done by the closed result set.
     */
    private void close(ResultSet resultSet, String task) {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                log.error("Closing result set failed after " + task, e);
            }
        }
    }
    
    
    /**
     * Closes the provided connection. on failure log the error;
     *
     * @param connection Connection
     * @param task       task that was done before closing
     */
    private void close(Connection connection, String task) {
        if (connection != null) {
            try {
                connection.setAutoCommit(true);
                connection.close();
            } catch (SQLException e) {
                log.error("Failed to close connection after " + task, e);
            }
        }
    }

    /**
     * On database update failure tries to rollback
     *
     * @param connection database connection
     * @param task       explanation of the task done when the rollback was triggered
     */
    private void rollback(Connection connection, String task) {
        if (connection != null) {
            try {
                connection.rollback();
            } catch (SQLException e) {
                log.warn("Rollback failed on " + task, e);
            }
        }
    }
    
    /**
     * Throws an {@link AndesBatchUpdateException} with specified
     * information. and rollback the tried batch operation.
     * 
     * @param dataList
     *            data objects will was used for batch operation.
     * @param connection
     *            SQL connection used.
     * @param bue
     *            the original SQL batch update exception.
     * @param task
     *            the string indicating the task tried out (in failed batch
     *            update operation)
     * @throws AndesBatchUpdateException
     *             the error.
     */
    public <T> void raiseBatchUpdateException(List<T> dataList, Connection connection,
                                              BatchUpdateException bue,
                                              String task) throws AndesBatchUpdateException {

        int[] updateCountsOfFailedBatch = bue.getUpdateCounts();
        List<T> failed = new ArrayList<T>();
        List<T> succeded = new ArrayList<T>();

        for (int i = 0; i < updateCountsOfFailedBatch.length; i++) {
            T msgPart = dataList.get(i);
            if (Statement.EXECUTE_FAILED == updateCountsOfFailedBatch[i]) {
                failed.add(msgPart);
            } else {
                succeded.add(msgPart);
            }
        }

        rollback(connection, task); // try to rollback the batch operation.

        AndesBatchUpdateException insertEx =
                                                new AndesBatchUpdateException(task + " failed", bue.getSQLState(),
                                                                                 bue, failed, succeded);
        throw insertEx;
    }

}
