package org.wso2.andes.subscription;

import org.wso2.andes.kernel.Subscrption;

import java.util.UUID;

public class BasicSubscription implements Subscrption{
	protected String subscriptionID; 
	protected String destination;
	protected boolean isBoundToTopic;
	protected boolean isExclusive; 
	protected boolean isDurable;
	protected String subscribedNode;
	protected String targetQueue;
    protected String targetQueueOwner;
    protected String targetQueueBoundExchange;
    protected String targetQueueBoundExchangeType;
    protected Short isTargetQueueBoundExchangeAutoDeletable;
    protected boolean hasExternalSubscriptions;
	

	public BasicSubscription(String subscriptionAsStr){
		String[] propertyToken = subscriptionAsStr.split(",");
		for(String pt: propertyToken){
			String[] tokens = pt.split("=");
			if(tokens[0].equals("subscriptionID")){
				this.subscriptionID = tokens[1];
			}else if(tokens[0].equals("destination")){
				this.destination = tokens[1];
			}else if(tokens[0].equals("isBoundToTopic")){
				this.isBoundToTopic = Boolean.parseBoolean(tokens[1]);
			}else if(tokens[0].equals("isExclusive")){
				this.isExclusive = Boolean.parseBoolean(tokens[1]);
			}else if(tokens[0].equals("isDurable")){
				this.isDurable = Boolean.parseBoolean(tokens[1]);
			}else if(tokens[0].equals("subscribedNode")){
				this.subscribedNode = tokens[1];
			}else if(tokens[0].equals("targetQueue")){
				this.targetQueue = tokens[1];
            }else if(tokens[0].equals("targetQueueOwner")){
                this.targetQueueOwner = tokens[1].equals("null") ? null : tokens[1] ;
            }else if(tokens[0].equals("targetQueueBoundExchange")){
                this.targetQueueBoundExchange =  tokens[1].equals("null") ? null : tokens[1] ;
            }else if(tokens[0].equals("targetQueueBoundExchangeType")){
                this.targetQueueBoundExchangeType =  tokens[1].equals("null") ? null : tokens[1] ;
            }else if(tokens[0].equals("isTargetQueueBoundExchangeAutoDeletable")){
                this.isTargetQueueBoundExchangeAutoDeletable =  tokens[1].equals("null") ? null : Short.parseShort(tokens[1]);
            }else if(tokens[0].equals("hasExternalSubscriptions")){
                this.hasExternalSubscriptions = Boolean.parseBoolean(tokens[1]);
			}else{
				if(tokens[0].trim().length() > 0){
					throw new UnsupportedOperationException("Unexpected token "+ tokens[0]); 
				}
			}
		}
	}
	
	public BasicSubscription(String subscriptionID, String destination,
			boolean isBoundToTopic, boolean isExclusive, boolean isDurable,
			String subscribedNode, String targetQueue, String targetQueueOwner, String targetQueueBoundExchange,
            String targetQueueBoundExchangeType, Short isTargetQueueBoundExchangeAutoDeletable, boolean hasExternalSubscriptions) {

		super();
		this.subscriptionID = subscriptionID;
		if(subscriptionID == null){
			this.subscriptionID = UUID.randomUUID().toString();
		}

		this.destination = destination;
		this.isBoundToTopic = isBoundToTopic;
		this.isExclusive = isExclusive;
		this.isDurable = isDurable;
		this.subscribedNode = subscribedNode;
		this.targetQueue = targetQueue;
        this.targetQueueOwner = targetQueueOwner;
        this.targetQueueBoundExchange = targetQueueBoundExchange;
        this.targetQueueBoundExchangeType = targetQueueBoundExchangeType;
        this.isTargetQueueBoundExchangeAutoDeletable = isTargetQueueBoundExchangeAutoDeletable;
        this.hasExternalSubscriptions = hasExternalSubscriptions;
	}

	@Override
	public String getSubscriptionID() {
		return subscriptionID;
	}

	@Override
	public String getSubscribedDestination() {
		return destination;
	}

	@Override
	public boolean isBoundToTopic() {
		return isBoundToTopic;
	}

	@Override
	public boolean isDurable() {
		return isDurable;
	}

	@Override
	public String getSubscribedNode() {
		return subscribedNode;
	}

	public boolean isExclusive() {
		return isExclusive;
	}

	public void setExclusive(boolean isExclusive) {
		this.isExclusive = isExclusive;
	}

	public String getTargetQueue() {
		return targetQueue;
	}

    @Override
    public String getTargetQueueOwner() {
        return targetQueueOwner;
    }

    @Override
    public String getTargetQueueBoundExchangeName() {
        return targetQueueBoundExchange;
    }

    @Override
    public String getTargetQueueBoundExchangeType() {
        return targetQueueBoundExchangeType;
    }

    @Override
    public Short ifTargetQueueBoundExchangeAutoDeletable() {
        return isTargetQueueBoundExchangeAutoDeletable;
    }

    @Override
    public boolean hasExternalSubscriptions() {
        return hasExternalSubscriptions;
    }


    @Override
	public String toString() {
		StringBuffer buf = new StringBuffer();
		buf.append("[").append(destination)
		.append("]ID=").append(subscriptionID)
		.append("@").append(subscribedNode)
		.append("/D=").append(isDurable)
        .append("/X=").append(isExclusive)
        .append("/O=").append(targetQueueOwner)
        .append("/E=").append(targetQueueBoundExchange)
        .append("/ET=").append(targetQueueBoundExchangeType)
        .append("/EUD=").append(isTargetQueueBoundExchangeAutoDeletable)
        .append("/S=").append(hasExternalSubscriptions);
		return buf.toString();
	}

	@Override
	public String encodeAsStr() {
		StringBuffer buf = new StringBuffer();
		buf.append("subscriptionID=").append(subscriptionID)
		.append(",destination=").append(destination)
		.append(",isBoundToTopic=").append(isBoundToTopic)
		.append(",isExclusive=").append(isExclusive)
		.append(",isDurable=").append(isDurable)
		.append(",targetQueue=").append(targetQueue)
        .append(",targetQueueOwner=").append(targetQueueOwner)
        .append(",targetQueueBoundExchange=").append(targetQueueBoundExchange)
        .append(",targetQueueBoundExchangeType=").append(targetQueueBoundExchangeType)
        .append(",isTargetQueueBoundExchangeAutoDeletable=").append(isTargetQueueBoundExchangeAutoDeletable)
		.append(",subscribedNode=").append(subscribedNode)
        .append(",hasExternalSubscriptions=").append(hasExternalSubscriptions);
		return buf.toString();
	}
}
