package org.wso2.andes.kernel;

public interface LocalSubscription extends AndesSubscription {

	public int getnotAckedMsgCount();

    public void sendMessageToSubscriber(AndesMessageMetadata messageMetadata)throws AndesException;

    public boolean isActive();

    public LocalSubscription createQueueToListentoTopic();
}
