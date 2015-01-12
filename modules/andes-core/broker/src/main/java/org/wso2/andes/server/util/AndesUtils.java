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

package org.wso2.andes.server.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.server.queue.QueueEntry;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class holds utility methods for Andes. Commonly
 * accessible methods for the whole broker are piled
 * here
 */
public class AndesUtils {

    private static Log log = LogFactory.getLog(AndesUtils.class);

    private static AndesUtils self;
    private int cassandraPort = 9160;
    private int cqlPort = 9042;

    //this constant will be used to prefix storage queue name for topics
    public final static String TOPIC_NODE_QUEUE_PREFIX =  "TopicQueue";

    //This will be used to co-relate between the message id used in the browser and the message id used internally in MB
    private static ConcurrentHashMap<String, Long> browserMessageIdCorrelater = new ConcurrentHashMap<String, Long>();

    private  static PrintWriter printWriterGlobal;

    public static AndesUtils getInstance() {
        if(self == null){
            self = new AndesUtils();
        }
        return self;
    }

    public static String printAMQMessage(QueueEntry message){
        ByteBuffer buf = ByteBuffer.allocate(100); 
        int readCount = message.getMessage().getContent(buf, 0);
        return "("+ message.getMessage().getMessageNumber() + ")" + new String(buf.array(),0, readCount); 
    }

    public int getCassandraPort() {
        return cassandraPort;
    }

    public void setCassandraPort(int cassandraPort) {
        this.cassandraPort = cassandraPort;
    }

	public final int getCqlPort() {
		return cqlPort;
	}

	public final void setCqlPort(int cqlPort) {
		this.cqlPort = cqlPort;
	}

    /**
     * Register a mapping between browser message Id and Andes message Id. This is expected to be invoked
     * whenever messages are passed to the browser via a browser subscription and is expecting a return from browser
     * with browser message Id which needs to be resolved to Andes Message Id.
     *
     * These mappings should be cleaned after they have served their purpose.
     *
     * @param browserMessageId The browser message Id / External message Id
     * @param andesMessageId Respective Andes message Id
     */
    public static synchronized void registerBrowserMessageId(String browserMessageId, long andesMessageId) {
        browserMessageIdCorrelater.put(browserMessageId, andesMessageId);
    }

    /**
     * Remove the register browser message Id - andes message Id mapping. This is expected to be invoked
     * when the relevant mapping is no longer valid or no longer required.
     *
     * @param browserMessageIdList The browser message Id / External message Id list to be cleaned.
     */
    public static synchronized void unregisterBrowserMessageIds(String[] browserMessageIdList) {
        for(String browserMessageId : browserMessageIdList) {
            long andesMessageId = browserMessageIdCorrelater.remove(browserMessageId);

            if(log.isDebugEnabled()) {
                log.debug("Browser message Id " + browserMessageId + " related to Andes message Id " + andesMessageId +
                    " was removed from browserMessageIdCorrelater");
            }
        }
    }

    /**
     * Get the respective Andes message Id for a given browser message Id.
     *
     * @param browserMessageId The browser message Id / External message Id
     * @return Andes message Id
     */
    public static synchronized Long getAndesMessageId(String browserMessageId) {
        Long andesMessageId;
        if (browserMessageIdCorrelater.containsKey(browserMessageId)) {
            andesMessageId = browserMessageIdCorrelater.get(browserMessageId);
        } else {
            andesMessageId = Long.valueOf(-1);
        }
        return andesMessageId;
    }

    public static void writeToFile(String whatToWrite, String filePath) {
        try {
            if(printWriterGlobal == null) {
                BufferedWriter bufferedWriter = new BufferedWriter( new FileWriter(filePath));
                printWriterGlobal = new PrintWriter(bufferedWriter);
            }

            printWriterGlobal.println(whatToWrite);

        } catch (IOException e) {
            System.out.println("Error. File to print received messages is not provided" + e);
        }

    }

    /**
     * Generate storage queue name for a given destination
     * @param destination subscribed routing key
     * @param nodeID id of this node
     * @param isTopic is destination represent a topic
     * @return  storage queue name for destination
     */
    public static String getStorageQueueForDestination(String destination, String nodeID, boolean isTopic) {
        String storageQueueName;
        // We need to add a prefix so that we could differentiate if queue is created under the same name
        //as topic
        if(isTopic) {
            storageQueueName = new StringBuilder("TOPIC_NODE_QUEUE_PREFIX").append("|").append(destination).append("|").append(nodeID).toString();
        } else {
            storageQueueName = destination;
        }
        return storageQueueName;
    }
}
