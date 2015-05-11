package org.wso2.andes.store;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import javax.mail.event.StoreListener;

import org.wso2.andes.kernel.AndesException;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * 
 * This class keeps track of all the {@link StoreListener}s and notifies when
 * store becomes (in-)operational.
 * 
 */
public class FailureObservingStoreManager {


    private static final int STORE_HEALTH_CHECK_INTERVAL_SECONDS = 10;

    /**
     * Initial delay for health check task.
     */
    private static final int STORE_HEALTH_CHECK_INITIAL_DELAY_SECONDS = 1;

    /**
     * All the {@link StoreHealthListener} implementations registed to recieve
     * callbacks.
     */
    private static Collection<StoreHealthListener> healthListeners =
                                                                     Collections.synchronizedCollection(new ArrayList<StoreHealthListener>());

    /**
     * A named thread factory build executor.
     */
    private final static ThreadFactory namedThreadFactory =
                                                            new ThreadFactoryBuilder().setNameFormat("FailureObservingStores-HealthCheckPool")
                                                                                      .build();
    /**
     * Executor used for health checking
     */
    private final static ScheduledExecutorService executor = Executors.newScheduledThreadPool(2, namedThreadFactory);

    /**
     * A Private constructor
     */
    private FailureObservingStoreManager() {
    }

    
    /**
     * Registers specified {@link StoreHealthListener} to receive events.
     * @param healthListener
     */
    public static void registerStoreHealthListener(StoreHealthListener healthListener){
        healthListeners.add(healthListener);
    }
    
    /**
     * Schedules a health check task for a in-operational for a {@link HealthAwareStore}
     * Note: visibility is at package level which is intentional.
     * @param store which became in-operational
     * @return a future referring to the periodic health check task.
     */
    static ScheduledFuture<?> scheduleHealthCheckTask(HealthAwareStore store) {
        StoreHealthCheckTask healthCheckTask = new StoreHealthCheckTask(store, healthListeners);
        return executor.scheduleWithFixedDelay(healthCheckTask, STORE_HEALTH_CHECK_INITIAL_DELAY_SECONDS,
                                               STORE_HEALTH_CHECK_INTERVAL_SECONDS, TimeUnit.SECONDS);
   }

    /**
     * A utility method to broadcast that a store became in-operational.
     * 
     * @param e
     *            error occurred
     * @param healthAwareStore
     *            in-operational store
     */
    synchronized static void notifyStoreInoperational(AndesException e, HealthAwareStore healthAwareStore) {
            // this is the first failure
            for (StoreHealthListener listener : healthListeners) {
                listener.storeInoperational(healthAwareStore, e);
            }
    }

    /**
     * A utility method to broadcast that a store became operational.
     * 
     * @param e
     *            error occurred
     * @param healthAwareStore
     *            the store which became operational.
     */
    synchronized static void notifyStoreOperational(HealthAwareStore healthAwareStore) {
        // this is the first failure
        for (StoreHealthListener listener : healthListeners) {
            listener.storeOperational(healthAwareStore);
        }

    }
}
