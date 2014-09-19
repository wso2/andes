package org.wso2.andes.kernel;

import com.hazelcast.core.HazelcastInstance;
import org.apache.axis2.clustering.ClusteringAgent;
import org.apache.axis2.clustering.management.NodeManager;
import org.wso2.andes.subscription.SubscriptionStore;

import java.util.Map;

public class AndesContext {
    private String messageStoreClass;
    private String andesContextStoreClass;
    private SubscriptionStore subscriptionStore;
    private AndesContextStore andesContextStore;
	private Map<String, AndesSubscription> dataSenderMap;
    private HazelcastInstance hazelcastInstance;
    private ClusteringAgent agent;
    private ClusteringAgent clusteringAgent;
    private boolean isClusteringEnabled;
    private AMQPConstructStore AMQPConstructStore;
    private static AndesContext instance = new AndesContext();
    private String messageStoreDataSourceName;
    private String contextStoreDataSourceName;
    private String thriftServerHost;
    private int thriftServerPort;


 	/**
     * @return fully qualified class name of a MessageStore interface implementation
     */
    public String getMessageStoreClass() {
        return messageStoreClass;
    }

    /**
     * Method works as a placeholder for fully qualified class name for the MessageStore implementation.
     * This is used to create the relevant MessageStore object
     *
     * @param messageStoreClass fully qualified class name of the MessageStore interface implementation
     */
    public void setMessageStoreClass(String messageStoreClass) {
        this.messageStoreClass = messageStoreClass;
    }

    /**
     * Used as a placeholder for fully qualified class name for the MessageStore implementation.
     * This is used to create the relevant context store object
     *
     * @param andesContextStoreClass fully qualified class name of a MessageStore interface implementation
     */
    public void setAndesContextStoreClass(String andesContextStoreClass) {
        this.andesContextStoreClass = andesContextStoreClass;
    }

    /**
     * @return fully qualified class name of a MessageStore interface implementation
     */
    public String getAndesContextStoreClass() {
        return andesContextStoreClass;
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

    public String getMessageStoreDataSourceName() {
        return messageStoreDataSourceName;
    }

    /**
     *
     * @param messageStoreDataSourceName data source name according to jndi config
     */
    public void setMessageStoreDataSourceName(String messageStoreDataSourceName) {
        this.messageStoreDataSourceName = messageStoreDataSourceName;
    }

    public String getContextStoreDataSourceName() {
        return contextStoreDataSourceName;
    }

    /**
     *
     * @param contextStoreDataSourceName data source name according to jndi config
     */
    public void setContextStoreDataSourceName(String contextStoreDataSourceName) {
        this.contextStoreDataSourceName = contextStoreDataSourceName;
    }

    public void setClusteringAgent(ClusteringAgent clusteringAgent){
      this.clusteringAgent = clusteringAgent;
    }

    public ClusteringAgent getClusteringAgent() {
        return clusteringAgent;
    }

    public String getThriftServerHost() {
        return thriftServerHost;
    }

    public void setThriftServerHost(String thriftServerHost) {
        this.thriftServerHost = thriftServerHost;
    }

    public int getThriftServerPort() {
        return thriftServerPort;
    }

    public void setThriftServerPort(int thriftServerPort) {
        this.thriftServerPort = thriftServerPort;
    }
}
