package org.dna.mqtt.moquette.messaging.spi.impl.events;

public class PubAckEvent extends MessagingEvent {

    int m_messageId;

    String m_clientID;

    public PubAckEvent(int messageID, String clientID) {
        m_messageId = messageID ;
        m_clientID = clientID;
    }

    public int getMessageId() {
        return m_messageId;
    }

    public String getClientID() {
        return m_clientID;
    }
}
