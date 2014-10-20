package org.dna.mqtt.moquette.proto.messages;

/**
 * Doesn't care DUP, QOS and RETAIN flags.
 * 
 * @author andrea
 */
public class DisconnectMessage extends ZeroLengthMessage {
    
    public DisconnectMessage() {
        m_messageType = AbstractMessage.DISCONNECT;
    }
}
