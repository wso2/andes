package org.wso2.andes.transport.flow.control;

import javax.management.Notification;
import javax.management.NotificationFilter;

public class MemoryMonitorNotificationFilter implements NotificationFilter {
    @Override
    public boolean isNotificationEnabled(Notification notification) {
        return true;
    }

}
