package org.wso2.andes.kernel;

import org.wso2.andes.subscription.SubscriptionStore;

import java.util.Map;

public class AndesContext {
    private String messageStoreClass;
    private String andesContextStoreClass;
    private SubscriptionStore subscriptionStore;
    private AndesContextStore andesContextStore;
    private Map<String, AndesSubscription> dataSenderMap;
    private boolean isClusteringEnabled;
    private String messageStoreDataSourceName;
    private String contextStoreDataSourceName;


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

	public SubscriptionStore getSubscriptionStore() {
		return subscriptionStore;
	}
	public void setSubscriptionStore(SubscriptionStore subscriptionStore) {
		this.subscriptionStore = subscriptionStore;
	}

    public void setAndesContextStore(AndesContextStore andesContextStore) {
        this.andesContextStore = andesContextStore;
    }

    public AndesContextStore getAndesContextStore() {
        return this.andesContextStore;
    }
	
	public void addDataSender(String key, AndesSubscription dataSender){
		dataSenderMap.put(key, dataSender);
	}
	
	public AndesSubscription getDataSender(String key){
		return dataSenderMap.get(key);
	}
	
	private static AndesContext instance = new AndesContext();
	public static AndesContext getInstance(){
		return instance;
	}

    public boolean isClusteringEnabled(){
        return isClusteringEnabled;
    }

    public void setClusteringEnabled(boolean isClusteringEnabled){
        this.isClusteringEnabled = isClusteringEnabled;
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
}
