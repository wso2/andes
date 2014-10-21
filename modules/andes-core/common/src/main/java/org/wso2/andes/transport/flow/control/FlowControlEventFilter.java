package org.wso2.andes.transport.flow.control;

import javax.management.Notification;
import javax.management.NotificationFilter;

public class FlowControlEventFilter implements NotificationFilter {

    public boolean isNotificationEnabled(Notification notification) {
        return notification.getType().equals(
                FlowControlConstants.FLOW_CONTROL_PER_CONNECTION_MESSAGE_THRESHOLD_EXCEEDED);
    }

}
