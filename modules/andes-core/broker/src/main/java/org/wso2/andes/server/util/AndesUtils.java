package org.wso2.andes.server.util;

import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.queue.QueueEntry;
import java.nio.ByteBuffer;
import java.util.ArrayList;

public class AndesUtils {

    private static AndesUtils self;
    private int cassandraPort = 9160;
    private int cqlPort = 9042;

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
        int nodeId = ClusterResourceHolder.getInstance().getClusterManager().getNodeId();
        String topicNodeQueueName = AndesConstants.TOPIC_NODE_QUEUE_NAME_PREFIX + nodeId;
        return topicNodeQueueName;
    }

    public static String getNodeQueueNameForNodeId(int nodeId) {
        String nodeQueueName = AndesConstants.NODE_QUEUE_NAME_PREFIX + nodeId;
        return nodeQueueName;
    }

    public static String getTopicNodeQueueNameForNodeId(int nodeId) {
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
    
    
}
