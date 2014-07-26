package org.wso2.andes.kernel;


public interface AndesSubscription {
    /**
     * @return subscription ID of the subscription or null
     */
	public String getSubscriptionID();

    /**
     * @return routing key of the binding to whom subscription is made
     */
	public String getSubscribedDestination();

    /**
     * @return is queue of this subscription bound to any topic
     */
	public boolean isBoundToTopic();

    /**
     * @return is subscribed to a durable queue/binding
     */
	public boolean isDurable();

    /**
     * @return get the node from which subscription is made or null
     */
	public String getSubscribedNode();

    /**
     * @return is subscribed queue is exclusive
     */
	public boolean isExclusive();

	public void setExclusive(boolean isExclusive);

    /**
     * Encode the object as a string
     * @return  encoded string
     */
	public String encodeAsStr();

    /**
     * @return  subscribed queue name
     */
	public String getTargetQueue();

    /**
     * @return owner of the subscribed queue
     */
    public String getTargetQueueOwner();

    /**
     * exchange subscribed queue is bound (for each binding we will be adding a subscription entry)
     * @return  exchange name subscribed queue is bound
     */
    public String getTargetQueueBoundExchangeName();

    /**
     *  exchange type subscribed queue is bound (for each binding we will be adding a subscription entry)
     * @return  exchange type subscribed queue is bound
     */
    public String getTargetQueueBoundExchangeType();

    /**
     * whether exchange is auto-deletable where subscribed queue is bound (for each binding we will be adding a subscription entry)
     * @return  whether exchange of the binding is auto-deletable
     */
    public Short ifTargetQueueBoundExchangeAutoDeletable();

    /**
     * @return  whether subscribed queue external subscription
     */
    public boolean hasExternalSubscriptions();
}
