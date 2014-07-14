package org.wso2.andes.server.virtualhost;

import org.wso2.andes.management.common.mbeans.ManagedAMQChannel;
import org.wso2.andes.server.AMQChannel;
import org.wso2.andes.server.management.AMQManagedObject;
import org.wso2.andes.transport.flow.control.FlowControlConstants;

import javax.management.MBeanNotificationInfo;
import javax.management.NotCompliantMBeanException;
import javax.management.Notification;
import javax.management.ObjectName;
import javax.management.monitor.MonitorNotification;

public class AMQChannelMBean extends AMQManagedObject implements ManagedAMQChannel {

    private AMQChannel channel;
    private long notificationSequence = 0;

    public AMQChannelMBean(AMQChannel channel) throws NotCompliantMBeanException {
        super(ManagedAMQChannel.class, ManagedAMQChannel.TYPE);
        this.channel = channel;
    }

    @Override
    public String getObjectInstanceName() {
        return ObjectName.quote(getName());
    }

    /**
     * Returns metadata of the Notifications sent by this MBean.
     */
    @Override
    public MBeanNotificationInfo[] getNotificationInfo()
    {
        String[] notificationTypes = new String[] { MonitorNotification.THRESHOLD_VALUE_EXCEEDED };
        String name = MonitorNotification.class.getName();
        String description = "Per connection message processing rate threshold exceeded";
        MBeanNotificationInfo info = new MBeanNotificationInfo(notificationTypes, name, description);

        return new MBeanNotificationInfo[] { info };
    }

    private AMQChannel getAMQChannel() {
        return channel;
    }

    @Override
    public void thresholdExceeded(int count) throws Exception {
        _broadcaster.sendNotification(
                new Notification(
                        FlowControlConstants.FLOW_CONTROL_PER_CONNECTION_MESSAGE_THRESHOLD_EXCEEDED,
                        this,
                        ++notificationSequence,
                        "Per connection message threshold exceeded"
                )
        );
    }

    @Override
    public String getName() {
        return "AMQChannel" + channel.getId();
    }

}
