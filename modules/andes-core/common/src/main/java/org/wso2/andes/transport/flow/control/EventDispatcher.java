package org.wso2.andes.transport.flow.control;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.management.Notification;
import javax.management.NotificationListener;
import java.lang.management.MemoryNotificationInfo;
import java.util.ArrayList;
import java.util.List;

public class EventDispatcher implements NotificationListener {

    private List<FlowControlEventObserver> listeners = new ArrayList<FlowControlEventObserver>();
    private static final Log log = LogFactory.getLog(EventDispatcher.class);

    public EventDispatcher() {
        super();
    }

    public void addObserver(FlowControlEventObserver listener) {
        this.getListeners().add(listener);
    }

    public void removeObserver(FlowControlEventObserver listener) {
        this.getListeners().remove(listener);
    }

    public void handleNotification(Notification notification, Object handback) {
        String type = notification.getType();
        try {
            if (MemoryNotificationInfo.MEMORY_COLLECTION_THRESHOLD_EXCEEDED.equals(type)) {
                log.debug("Memory Threshold Exceeded Notification Received");
                for (FlowControlEventObserver listener : this.getListeners()) {
                    listener.update(
                            FlowControlEventObserver.FlowControlState.MEMORY_THRESHOLD_EXCEEDED);
                }
            } else if (FlowControlConstants.FLOW_CONTROL_PER_CONNECTION_MESSAGE_THRESHOLD_EXCEEDED.equals(type)){
                log.debug("Per Connection message Threshold Exceeded Notification Received");
                /* TODO: selective notifications */
                for (FlowControlEventObserver listener : this.getListeners()) {
                    listener.update(
                            FlowControlEventObserver.FlowControlState.
                                    PER_CONNECTION_MESSAGE_THRESHOLD_EXCEEDED);
                }
            } else if (FlowControlConstants.FLOW_CONTROL_MEMORY_THRESHOLD_RECOVERED.equals(type)) {
                log.debug("Memory Threshold Recovered Notification Received");
                for (FlowControlEventObserver listener : this.getListeners()) {
                    listener.update(
                            FlowControlEventObserver.FlowControlState.MEMORY_THRESHOLD_RECOVERED);
                }
            }
        } catch (Exception e) {
            //TODO: Handle exception properly
        }
    }

    private List<FlowControlEventObserver> getListeners() {
        return listeners;
    }

}
