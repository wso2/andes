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

package org.wso2.andes.store.rdbms;

import org.wso2.andes.kernel.AndesBinding;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.AndesMessagePart;
import org.wso2.andes.kernel.AndesQueue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;

public class RDBMSTestHelper {

    protected static List<AndesMessageMetadata> getMetadataList(final String destQueueName, long firstMsgId, long lastMsgId) {
        return getMetadataList(destQueueName, firstMsgId, lastMsgId, 10000);
    }

    protected static List<AndesMessageMetadata> getMetadataList(final String destQueueName, long firstMsgId,
                                                                long lastMsgId, int expirationTime) {
        List<AndesMessageMetadata> lst = new ArrayList<AndesMessageMetadata>(10);
        for (long i = firstMsgId; i < lastMsgId; i++) {
            lst.add(getMetadata(i, destQueueName, expirationTime));
        }
        return lst;
    }
    /**
     *
     * @param qNameArray String array of queue names
     * @param divisor modulo division is done to select the queue name for each message while iterating from firstMsgId
     *                to lastMsgId
     * @param firstMsgId firstMsgId
     * @param lastMsgId lastMsgId
     * @return List of AndesMessageMetadata
     */
    protected static List<AndesMessageMetadata> getMetadataForMultipleQueues(final String[] qNameArray, int divisor,
                                                                             long firstMsgId, long lastMsgId) {
        List<AndesMessageMetadata> lst = new ArrayList<AndesMessageMetadata>(10);
        for (long i = firstMsgId; i < lastMsgId; i++) {
            lst.add(getMetadata(i, qNameArray[(int) i % divisor]));
        }
        return lst;
    }

    protected static AndesMessageMetadata getMetadata(long msgId, final String queueName) {
        return getMetadata(msgId, queueName, 10000);
    }

    protected static AndesMessageMetadata getMetadata(long msgId, final String queueName, int expirationTime) {
        AndesMessageMetadata md = new AndesMessageMetadata();
        md.setMessageID(msgId);
        md.setDestination(queueName);
        md.setStorageQueueName(queueName);
        md.setMetadata(("\u0002:MessageID=" + msgId + ",persistent=false,Topic=false,Destination=" + queueName +
                ",Persistant=false,MessageContentLength=0").getBytes());
        md.setExpirationTime((msgId % 2 == 0) ? (System.currentTimeMillis() + expirationTime) : 0);
        return md;
    }

    /**
     * Returns a List of AndesMessagePart including the first message id with two content chunks
     * for each message id
     * @param firstMsgId first message id
     * @param lastMsgId  last message id
     * @return
     */
    protected static List<AndesMessagePart> getMessagePartList(long firstMsgId, long lastMsgId) {
        List<AndesMessagePart> list = new ArrayList<AndesMessagePart>();
        byte[] content = "test message".getBytes();
        byte[] content2 = "second part".getBytes();

        for (long i = firstMsgId; i < lastMsgId; i++) {
            AndesMessagePart p = new AndesMessagePart();
            p.setMessageID(i);
            p.setData(content);
            p.setDataLength(content.length);
            p.setOffSet(0);
            list.add(p);

            int offset = content.length;
            p = new AndesMessagePart();
            p.setMessageID(i);
            p.setData(content2);
            p.setDataLength(content2.length);
            p.setOffSet(offset);
            list.add(p);
        }
        return list;
    }

    /**
     * Stores a binding in store with relevant exchange and queue information
     * @param connection Connection
     * @param exchange name of exchange
     * @param boundQueue bounded queue
     * @param routingKey routing key
     * @param owner owner of the queue
     * @throws Exception
     */
    protected static void storeBindingInfo(Connection connection, String exchange,
                                           String boundQueue,
                                           String routingKey,
                                           String owner  ) throws Exception {
        // store queue and exchange information accordingly in db before storing binging.
        AndesQueue andesQueue = new AndesQueue(boundQueue, "owner1", true, false);
        AndesBinding andesBinding = new AndesBinding(exchange, andesQueue, routingKey);
        // setup database with queue information
        String insert = "INSERT INTO " + RDBMSConstants.QUEUE_INFO_TABLE + " (" +
                RDBMSConstants.QUEUE_NAME + "," + RDBMSConstants.QUEUE_INFO + " ) " +
                " VALUES (?,?)";

        PreparedStatement preparedStatement = connection.prepareStatement(insert);
        preparedStatement.setString(1, andesQueue.queueName);
        preparedStatement.setString(2, andesQueue.encodeAsString());
        preparedStatement.addBatch();

        preparedStatement.executeBatch();
        preparedStatement.close();

        // setup database with exchange information
        String exchangeInfo = "exchangeName=" + exchange + ",type=none," +
                "autoDelete=false";

        insert = "INSERT INTO " + RDBMSConstants.EXCHANGES_TABLE + " ( " +
                RDBMSConstants.EXCHANGE_NAME + "," +
                RDBMSConstants.EXCHANGE_DATA + ") " +
                " VALUES (?, ?)";

        preparedStatement = connection.prepareStatement(insert);
        preparedStatement.setString(1, exchange);
        preparedStatement.setString(2, exchangeInfo);
        preparedStatement.addBatch();
        preparedStatement.executeBatch();
        preparedStatement.close();

        // setup database with binding information
        insert = "INSERT INTO " + RDBMSConstants.BINDINGS_TABLE + " (" +
                RDBMSConstants.BINDING_EXCHANGE_NAME + "," +
                RDBMSConstants.BINDING_QUEUE_NAME + "," +
                RDBMSConstants.BINDING_INFO + " ) " +
                " VALUES (?,?,?)";

        preparedStatement = connection.prepareStatement(insert);
        preparedStatement.setString(1, exchange);
        preparedStatement.setString(2, andesQueue.queueName);
        preparedStatement.setString(3, andesBinding.encodeAsString());
        preparedStatement.addBatch();
        preparedStatement.executeBatch();

    }
}
