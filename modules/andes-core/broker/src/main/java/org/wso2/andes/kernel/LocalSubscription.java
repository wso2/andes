package org.wso2.andes.kernel;

public interface LocalSubscription extends Subscrption{

	public int getnotAckedMsgCount();

    public void sendMessageToSubscriber(AndesMessageMetadata messageMetadata)throws AndesException;

    public boolean isActive();

    public LocalSubscription createQueueToListentoTopic();
}
