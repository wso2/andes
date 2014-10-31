package org.wso2.andes.kernel;

/**
 * This interface have methods to implement to get messages from andes to other components
 */
public interface StatPublisherMessageListener {

    public void sendMessageDetails(AndesMessageMetadata message, int noOfSubscribers);
    public void sendAckMessageDetails(AndesAckData ack);

}
