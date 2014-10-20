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
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.queue.QueueEntry;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

public class AndesUtils {

    private static Log log = LogFactory.getLog(AndesUtils.class);

    private static AndesUtils self;
    private int cassandraPort = 9160;
    private int cqlPort = 9042;

    //This will be used to co-relate between the message id used in the browser and the message id used internally in MB
    private static ConcurrentHashMap<String, Long> browserMessageIdCorrelater = new ConcurrentHashMap<String, Long>();

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


    /**
     * Calculate the name of the global queue , with using the queue name of the message
     * passed in to the method.It will get the hash code of the passed queue name
     * and get mod value of it after dividing by the number of available global queue
     * and append that value to the string "GlobalQueue_"
     *
     * Eg: if the mod value is 7, global queue name will be : GlobalQueue_7
     * @param destinationQueueName - Name of the queue that require to calculate the global queue
     * @return globalQueueName - Name of the global queue
     * */
    public  static String getGlobalQueueNameForDestinationQueue(String destinationQueueName) {
        int globalQueueCount = ClusterResourceHolder.getInstance().getClusterConfiguration().getGlobalQueueCount();
        int globalQueueId = Math.abs(destinationQueueName.hashCode()) % globalQueueCount;
        return AndesConstants.GLOBAL_QUEUE_NAME_PREFIX + globalQueueId;
    }

    /**
     * Gets all the names of the global queue according the user configured global queue count
     * @return list of global queue names
     * */
    public static ArrayList<String> getAllGlobalQueueNames(){
        ArrayList<String> globalQueueNames = new ArrayList<String>();
        int globalQueueCount = ClusterResourceHolder.getInstance().getClusterConfiguration().getGlobalQueueCount();
        for(int i=0; i < globalQueueCount; i ++ ){
            globalQueueNames.add(AndesConstants.GLOBAL_QUEUE_NAME_PREFIX + i);
        }
        return globalQueueNames;
    }

    public static String getTopicNodeQueueName(){
        String nodeId = ClusterResourceHolder.getInstance().getClusterManager().getMyNodeID();
        String topicNodeQueueName = AndesConstants.TOPIC_NODE_QUEUE_NAME_PREFIX + nodeId;
        return topicNodeQueueName;
    }

    public static String getNodeQueueNameForNodeId(String nodeId) {
        String nodeQueueName = AndesConstants.NODE_QUEUE_NAME_PREFIX + nodeId;
        return nodeQueueName;
    }

    public static String getTopicNodeQueueNameForNodeId(String nodeId) {
        String topicNodeQueueName = AndesConstants.TOPIC_NODE_QUEUE_NAME_PREFIX + nodeId;
        return topicNodeQueueName;
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
    
    
}
