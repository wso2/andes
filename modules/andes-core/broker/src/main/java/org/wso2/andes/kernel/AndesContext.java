package org.wso2.andes.kernel;

import com.hazelcast.core.HazelcastInstance;
import org.wso2.andes.subscription.SubscriptionStore;

import java.util.Map;

public class AndesContext {
    private MessageStore messageStore;
    private SubscriptionStore subscriptionStore;
    private AndesContextStore andesContextStore;
    private Map<String, AndesSubscription> dataSenderMap;
    private HazelcastInstance hazelcastInstance;
    private boolean isClusteringEnabled;
    private AMQPConstructStore AMQPConstructStore;
    private static AndesContext instance = new AndesContext();

    /**
     * get message store
     *
     * @return MessageStore
     */
    public MessageStore getMessageStore() {
        return messageStore;
    }

    /**
     * set message store instance
     *
     * @param messageStore message store to keep
     */
    public void setMessageStore(MessageStore messageStore) {
        this.messageStore = messageStore;
    }

    /**
     * get subscription store
     *
     * @return subscription store
     */
    public SubscriptionStore getSubscriptionStore() {
        return subscriptionStore;
    }

    /**
     * set subscription store
     *
     * @param subscriptionStore subscription store
     */
    public void setSubscriptionStore(SubscriptionStore subscriptionStore) {
        this.subscriptionStore = subscriptionStore;
    }

    /**
     * set andes context store
     *
     * @param andesContextStore context store to store
     */
    public void setAndesContextStore(AndesContextStore andesContextStore) {
        this.andesContextStore = andesContextStore;
    }

    /**
     * get andes context store
     *
     * @return context store
     */
    public AndesContextStore getAndesContextStore() {
        return this.andesContextStore;
    }

    public void addDataSender(String key, AndesSubscription dataSender) {
        dataSenderMap.put(key, dataSender);
    }

    public AndesSubscription getDataSender(String key) {
        return dataSenderMap.get(key);
    }

    /**
     * get andes context instance
     *
     * @return andes context
     */
    public static AndesContext getInstance() {
        return instance;
    }

    /**
     * get hazleCast instance
     *
     * @return hazelcast instance
     */
    public HazelcastInstance getHazelcastInstance() {
        return this.hazelcastInstance;
    }

    /**
     * set hazelCast instance
     *
     * @param hazelcastInstance
     */
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }

    /**
     * get if clustering is enabled
     *
     * @return if clustering is on
     */
    public boolean isClusteringEnabled() {
        return isClusteringEnabled;
    }

    /**
     * set if clustering is enabled
     *
     * @param isClusteringEnabled if clustering is enabled
     */
    public void setClusteringEnabled(boolean isClusteringEnabled) {
        this.isClusteringEnabled = isClusteringEnabled;
    }

    /**
     * set AMQP constructs store instance
     *
     * @param AMQPConstructStore AMQP constructs store
     */
    public void setAMQPConstructStore(AMQPConstructStore AMQPConstructStore) {
        this.AMQPConstructStore = AMQPConstructStore;
    }

    /**
     * get AMQP construct store
     *
     * @return AMQP construct store
     */
    public AMQPConstructStore getAMQPConstructStore() {
        return AMQPConstructStore;
    }
}
