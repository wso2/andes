package org.dna.mqtt.wso2;

import org.dna.mqtt.moquette.messaging.spi.impl.ProtocolProcessor;
import org.dna.mqtt.moquette.proto.messages.AbstractMessage;
import org.wso2.andes.mqtt.MQTTChannel;
import org.wso2.andes.mqtt.MQTTUtils;

import java.nio.ByteBuffer;
import java.util.HashMap;

public class AndesBridge {

    //The connection between the MQTT library
    private ProtocolProcessor localProcessor;
    //Will manage the relation between each topic with andes connection
    private HashMap<String, String> topics = new HashMap<String, String>();
    //Will manage the releationship between each subscriber to topic
    private HashMap<String, String> clientTopicRelation = new HashMap<String, String>();

    /*Will initialize the protocol object to take control of*/
    public AndesBridge(ProtocolProcessor processor) {
        this.localProcessor = processor;
    }

    /*Indicate Unsubscription*/
    public void indicateSubscriberDisconnection(String client_id) {
        //todo need to analyse the possibility of getting dead locked
        synchronized (clientTopicRelation) {
            if (clientTopicRelation.containsKey(client_id)) {
                String subscribe_topic = clientTopicRelation.get(client_id);
                clientTopicRelation.remove(client_id);
                //Per topic there will only be a single connecitons
                //Need to ensure that all the connections are removed to totally disconnect from topic
                //After removal if there's no indication of the topic existance we could remove
                if (!clientTopicRelation.containsValue(subscribe_topic)) {
                    //No relation to the topic means no connections
                    synchronized (topics) {
                        if (topics.containsKey(subscribe_topic)) {
                            //Will get the actual subscriber connection
                            String bridgingClientID = topics.get(subscribe_topic);
                            MQTTChannel.getInstance().disconnectSubscriber(this,subscribe_topic, bridgingClientID);
                            //Finally remove the entry from the list
                            topics.remove(subscribe_topic);
                        }
                    }
                }

            }
        }
    }

    /*Will add the message information to the kernal*/
    public static void addMessageInformationToAndes(String topic, String qos_level, ByteBuffer message, boolean retain, int messageID) {
        long generated_Message_id = MQTTUtils.generateMessageID();
        MQTTChannel.getInstance().handleMessageInstance(message, generated_Message_id, topic);
    }


    /*Will indicate on a subscription*/
    /*Per topic only one subscription will be placed*/
    public void indicateSubscription(String topic, String clientID) {
        synchronized (topics) {
            if (!topics.containsKey(topic)) {
                //Will generate a unique id for the client
                String bridgingClientID = MQTTUtils.generateBridgingClientID();
                MQTTChannel.getInstance().insertSubscriber(this, topic, bridgingClientID);
                topics.put(topic, bridgingClientID);
                //Will indicate the relation
                //clientTopicRelation.put(clientID,topic);
            }
        }

        synchronized (clientTopicRelation) {
            //Will maintain the relationship between the subscribers to topic
            clientTopicRelation.put(clientID, topic);
        }
    }

    /*Will be incharage of sending the message to the subscribers in MQTT layer */
    public void sendMessageToLocalProcessorForSubscription(String topic, String qos, ByteBuffer message, boolean retain, long messageID) {
        if (localProcessor != null) {
            //Need to set do a re possition of bytes for writing to the buffer
            message.position(0);
            localProcessor.publish2Subscribers(topic, AbstractMessage.QOSType.MOST_ONE, message, retain, (int) messageID);
        }
    }
}
