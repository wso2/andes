package org.wso2.andes.transport.flow.control;

public interface FlowControlEventObserver {

    public enum FlowControlState {
        MEMORY_THRESHOLD_EXCEEDED, PER_CONNECTION_MESSAGE_THRESHOLD_EXCEEDED,
        MEMORY_THRESHOLD_RECOVERED, PER_CONNECTION_MESSAGE_THRESHOLD_RECOVERED
    }

    void update(FlowControlState state) throws Exception;

}
