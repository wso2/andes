package org.dna.mqtt.moquette.messaging.spi.impl.subscriptions;

import org.dna.mqtt.moquette.proto.messages.AbstractMessage.QOSType;

import java.io.Serializable;

/**
 * Maintain the information about which Topic a certain ClientID is subscribed 
 * and at which QoS
 *
 */
public class Subscription implements Serializable {
    
    QOSType requestedQos;
    String clientId;
    String topic;
    boolean cleanSession;
    boolean active = true;
    
    public Subscription(String clientId, String topic, QOSType requestedQos, boolean cleanSession) {
        this.requestedQos = requestedQos;
        this.clientId = clientId;
        this.topic = topic;
        this.cleanSession = cleanSession;
    }

    public String getClientId() {
        return clientId;
    }

    public QOSType getRequestedQos() {
        return requestedQos;
    }

    public String getTopic() {
        return topic;
    }

    public boolean isCleanSession() {
        return this.cleanSession;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Subscription other = (Subscription) obj;
        if (this.requestedQos != other.requestedQos) {
            return false;
        }
        if ((this.clientId == null) ? (other.clientId != null) : !this.clientId.equals(other.clientId)) {
            return false;
        }
        if ((this.topic == null) ? (other.topic != null) : !this.topic.equals(other.topic)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 37 * hash + (this.requestedQos != null ? this.requestedQos.hashCode() : 0);
        hash = 37 * hash + (this.clientId != null ? this.clientId.hashCode() : 0);
        hash = 37 * hash + (this.topic != null ? this.topic.hashCode() : 0);
        return hash;
    }

    /**
     * Trivial match method
     */
    boolean match(String topic) {
        return this.topic.equals(topic);
    }
    
    @Override
    public String toString() {
        return String.format("[t:%s, cliID: %s, qos: %s, active: %s]", this.topic, this.clientId, this.requestedQos, this.active);
    }
}
