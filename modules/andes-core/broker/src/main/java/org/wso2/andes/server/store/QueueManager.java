package org.wso2.andes.server.store;


import org.wso2.andes.server.message.AMQMessage;
import org.wso2.andes.server.queue.AMQQueue;

public interface QueueManager {

    public void createQueue(AMQQueue amqQueue);

    public AMQQueue getQueue(String queueName);

    public void addMessage(AMQQueue queue, AMQMessage message);

    public AMQMessage popQueue(String queueName);

}
