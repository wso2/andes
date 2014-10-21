package org.dna.mqtt.moquette.messaging.spi.impl.events;

public class RepublishEvent extends MessagingEvent {
    private String m_clientID;

    public RepublishEvent(String clientID) {
        this.m_clientID = clientID;
    }

    public String getClientID() {
        return m_clientID;
    }
}
