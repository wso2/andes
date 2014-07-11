package org.wso2.andes.transport.flow.control;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.management.*;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.util.List;

public final class MemoryMonitor implements MemoryMonitorMBean, NotificationBroadcaster {

    private static Log log = LogFactory.getLog(MemoryMonitor.class);
    private NotificationBroadcasterSupport broadcaster = new NotificationBroadcasterSupport();
    private Thread task;

    public MemoryMonitor(final double recoveryThreshold, final long memoryCheckInterval) {
        super();
        this.task =
                new Thread(new MemoryMonitorTask(recoveryThreshold, broadcaster, memoryCheckInterval));
    }

    public void start() {
        task.start();
    }

    public void stop() {
        task.stop();
    }

    private static final class MemoryMonitorTask implements Runnable {

        private List<MemoryPoolMXBean> pools;
        private NotificationBroadcasterSupport broadcaster;
        private int notificationSequence;
        private double recoveryThreshold;
        private long memoryCheckInterval = 20000L;

        public MemoryMonitorTask(final double recoveryThreshold,
                                 final NotificationBroadcasterSupport broadcaster,
                                 final long memoryCheckInterval) {
            this.pools = ManagementFactory.getMemoryPoolMXBeans();
            this.broadcaster = broadcaster;
            this.recoveryThreshold = recoveryThreshold;
            this.memoryCheckInterval = memoryCheckInterval;
        }

        @Override
        public void run() {
            boolean memoryAvailable = false;
            while (true) {
                if (log.isDebugEnabled()) {
                    log.debug("Memory Monitor Is Running");
                }
                for (MemoryPoolMXBean poolMXBean : pools) {
                    if (MemoryType.HEAP.equals(poolMXBean.getType())) {
                        if (!poolMXBean.isUsageThresholdSupported()) {
                            continue;
                        }
                        MemoryUsage usage = poolMXBean.getUsage();
                        long thresholdMemory = (long) Math.floor(usage.getCommitted() * recoveryThreshold);
                        long availableMemory = (usage.getCommitted() - usage.getUsed());
                        if (log.isDebugEnabled()) {
                            log.debug("Maximum Amount Of Memory Available To Be Managed : " + usage.getMax() + " bytes");
                            log.debug("Memory Recovery Threshold : " + thresholdMemory + " bytes");
                            log.debug("Available Memory : " + availableMemory + " bytes");
                        }
                        if (availableMemory >= thresholdMemory) {
                            memoryAvailable = true;
                            break;
                        }
                    }
                }

                if (memoryAvailable) {
                    if (log.isDebugEnabled()) {
                        log.debug("Memory Recovery Notification Is Sent");
                    }
                    broadcaster.sendNotification(new Notification(
                            FlowControlConstants.FLOW_CONTROL_MEMORY_THRESHOLD_RECOVERED,
                            this, ++notificationSequence,
                            "Flow Control Memory Threshold Recovered"));
                }

                try {
                    Thread.sleep(memoryCheckInterval);
                } catch (InterruptedException ignored) {
                    //Do nothing
                }
                memoryAvailable = false;
            }
        }
    }


    @Override
    public void addNotificationListener(NotificationListener listener, NotificationFilter filter,
                                        Object handback) throws IllegalArgumentException {
        broadcaster.addNotificationListener(listener, filter, handback);
    }

    @Override
    public void removeNotificationListener(
            NotificationListener listener) throws ListenerNotFoundException {
        broadcaster.removeNotificationListener(listener);
    }

    @Override
    public MBeanNotificationInfo[] getNotificationInfo() {
        return new MBeanNotificationInfo[] {
                new MBeanNotificationInfo(
                        new String[]
                                { FlowControlConstants.FLOW_CONTROL_MEMORY_THRESHOLD_RECOVERED },
                        Notification.class.getName(),
                        "Flow Control Memory Threshold Recovered"
                )
        };
    }

}
