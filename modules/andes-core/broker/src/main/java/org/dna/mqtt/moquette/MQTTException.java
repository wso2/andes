package org.dna.mqtt.moquette;

public class MQTTException extends RuntimeException {

    public MQTTException() {
        super();
    }
    
    public MQTTException(String msg) {
        super(msg);
    }
    
    public MQTTException(Throwable cause) {
        super(cause);
    }

    public MQTTException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
