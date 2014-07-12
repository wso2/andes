package org.wso2.andes.mqtt;

import org.dna.mqtt.wso2.AndesBridge;
import org.wso2.andes.kernel.*;

import java.nio.ByteBuffer;
import java.util.Collection;


public class MQTTChannel {

    private static MQTTChannel channel;
    //private AndesBridge tem_bridge;

    /*
    *Will produce a new instance
    * Wil interface betweeen the kernal and the MQTT library*/
    public static synchronized MQTTChannel getInstance() {
        if (channel == null) {
            channel = new MQTTChannel();
        }

        return channel;
    }

    /*
    * Will insert the message body to the kernal*/
    private void addMessageBody(AndesMessagePart messageBody) {
        MessagingEngine.getInstance().messageContentReceived(messageBody);
    }

    /*Will insert the message header to the kernal*/
    private void addMessageHeader(AndesMessageMetadata messageHeader) {
        try {
            MessagingEngine.getInstance().messageReceived(messageHeader, (long) 1.0);
        } catch (AndesException e) {
            e.printStackTrace();
        }
    }

    /*Will get the message from the MQTT library*/
    public void handleMessageInstance(ByteBuffer pub_message, long msgID, String topic) {
        ByteBuffer message = pub_message;
        long messageID = msgID;
        //Will start converting the message body
        // AndesMessagePart msg = convertMessageBody(message,messageID);
        AndesMessagePart msg = MQTTUtils.convertToAndesMessage(message, messageID);
        //Will Create the Andes Header
        AndesMessageMetadata metaHeader = MQTTUtils.convertToAndesHeader(messageID, topic, pub_message.array().length);
        //Will write the message body
        addMessageBody(msg);
        //Will add the message header
        addMessageHeader(metaHeader);
    }

    /*Will handle the subscription
    * Keeps the server channel*/
    public void insertSubscriber(AndesBridge channel, String topic, String clientID) {
        //To-DO Need to check whether subscription to the topic has being created

        //Will create a new local subscription object
        MQTTLocalSubscription localSubscription = new MQTTLocalSubscription("");
        localSubscription.setMqqtServerChannel(channel);
        localSubscription.setTopic(topic);
        localSubscription.setSubscriptionID(clientID);

        //Shold indicate the record in the cluster
        try {
            AndesContext.getInstance().getSubscriptionStore().addLocalSubscription(localSubscription);
        } catch (AndesException e) {
            e.printStackTrace();
        }
    }

    /*Will handle subscriber disconnection*/
    public void disconnectSubscriber(String subscribedTopic, String clientID) {
        try {
            Collection<LocalSubscription> subscriptions = AndesContext.getInstance().getSubscriptionStore().getLocalSubscribersForTopic(subscribedTopic);
            for (LocalSubscription local_sub : subscriptions) {
                if (local_sub.getSubscriptionID().equals(clientID)) {
                    AndesContext.getInstance().getSubscriptionStore().closeLocalSubscription(local_sub);
                }
            }
        } catch (AndesException e) {
            e.printStackTrace();
        }
    }


}
