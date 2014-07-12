package org.wso2.andes.kernel;

import org.wso2.andes.subscription.SubscriptionStore;

import java.util.Map;

public class AndesContext {
	private MessageStore messageStore; 
	private SubscriptionStore subscriptionStore;
    private AndesContextStore andesContextStore;
	private Map<String, Subscrption> dataSenderMap;
	public MessageStore getMessageStore() {
		return messageStore;
	}
	public void setMessageStore(MessageStore messageStore) {
		this.messageStore = messageStore;
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
	
	public void addDataSender(String key, Subscrption dataSender){
		dataSenderMap.put(key, dataSender);
	}
	
	public Subscrption getDataSender(String key){
		return dataSenderMap.get(key);
	}
	
	private static AndesContext instance = new AndesContext();
	public static AndesContext getInstance(){
		return instance;
	}
	
}
