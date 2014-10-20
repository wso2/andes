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
package org.wso2.andes.server.store;

import org.wso2.andes.server.logging.LogSubject;
import org.wso2.andes.AMQStoreException;
import org.apache.commons.configuration.Configuration;

public interface TransactionLog
{

    public static interface Transaction
    {
        /**
         * Places a message onto a specified queue, in a given transactional context.
         *
         * @param queue     The queue to place the message on.
         * @param messageId The message to enqueue.
         * @throws AMQStoreException If the operation fails for any reason.
         */
        void enqueueMessage(TransactionLogResource queue, Long messageId) throws AMQStoreException;

        /**
         * Extracts a message from a specified queue, in a given transactional context.
         *
         * @param queue     The queue to place the message on.
         * @param messageId The message to dequeue.
         * @throws AMQStoreException If the operation fails for any reason, or if the specified message does not exist.
         */
        void dequeueMessage(TransactionLogResource queue, Long messageId) throws AMQStoreException;


        /**
         * Commits all operations performed within a given transactional context.
         *
         * @throws AMQStoreException If the operation fails for any reason.
         */
        void commitTran() throws AMQStoreException;

        /**
         * Commits all operations performed within a given transactional context.
         *
         * @throws AMQStoreException If the operation fails for any reason.
         */
        StoreFuture commitTranAsync() throws AMQStoreException;

        /**
         * Abandons all operations performed within a given transactional context.
         *
         * @throws AMQStoreException If the operation fails for any reason.
         */
        void abortTran() throws AMQStoreException;



    }

    public void configureTransactionLog(String name,
                      TransactionLogRecoveryHandler recoveryHandler,
                      Configuration storeConfiguration,
                      LogSubject logSubject) throws Exception;

    Transaction newTransaction();



    public static interface StoreFuture
    {
        boolean isComplete();

        void waitForCompletion();
    }
}
