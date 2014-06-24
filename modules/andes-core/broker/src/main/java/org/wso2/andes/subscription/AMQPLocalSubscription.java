package org.wso2.andes.subscription;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.AMQException;
import org.wso2.andes.amqp.AMQPUtils;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.LocalSubscription;
import org.wso2.andes.kernel.MessagingEngine;
import org.wso2.andes.server.AMQChannel;
import org.wso2.andes.server.binding.Binding;
import org.wso2.andes.server.message.AMQMessage;
import org.wso2.andes.server.protocol.AMQProtocolSession;
import org.wso2.andes.server.queue.AMQQueue;
import org.wso2.andes.server.queue.QueueEntry;
import org.wso2.andes.server.subscription.Subscription;
import org.wso2.andes.server.subscription.SubscriptionImpl;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AMQPLocalSubscription extends BasicSubscription implements LocalSubscription{

    private AMQQueue amqQueue;
	private Subscription amqpSubscription;
	AMQChannel channel = null;
    private static Log log = LogFactory.getLog(AMQPLocalSubscription.class);

	public AMQPLocalSubscription(AMQQueue amqQueue, Subscription amqpSubscription, String subscriptionID, String destination,
			boolean isBoundToTopic, boolean isExclusive, boolean isDurable,
			String subscribedNode, String targetQueue, String targetQueueOwner,
            String targetQueueBoundExchange, boolean hasExternalSubscriptions) {
		super(subscriptionID, destination, isBoundToTopic, isExclusive, isDurable, subscribedNode, targetQueue, targetQueueOwner, targetQueueBoundExchange, hasExternalSubscriptions);
		this.amqQueue = amqQueue;
		this.amqpSubscription = amqpSubscription;

        if(amqpSubscription !=null && amqpSubscription instanceof SubscriptionImpl.AckSubscription){
            channel = ((SubscriptionImpl.AckSubscription)amqpSubscription).getChannel();
        }
	}
	
	public int getnotAckedMsgCount(){
		return channel.getNotAckedMessageCount();
	}

	public boolean isActive(){
		return amqpSubscription.isActive();
	}

	@Override
	public void sendMessageToSubscriber(AndesMessageMetadata messageMetadata) throws AndesException {
		 AMQMessage message = AMQPUtils.getAMQMessageFromAndesMetaData(messageMetadata);
         sendAMQMessageToSubscriber(message, messageMetadata.getRedelivered());
	}


    private void sendAMQMessageToSubscriber(AMQMessage message, boolean isRedelivery) throws AndesException {
        QueueEntry messageToSend = AMQPUtils.convertAMQMessageToQueueEntry(message,amqQueue);
        if(isRedelivery) {
            messageToSend.setRedelivered();
        }
        sendQueueEntryToSubscriber(messageToSend);
    }

	private void sendQueueEntryToSubscriber(QueueEntry message) throws AndesException {
        if(message.getQueue().checkIfBoundToTopicExchange()) {
            //topic messages should be sent to all matching subscriptions
            String routingKey = message.getMessage().getRoutingKey();
            for(Binding bindingOfQueue : amqQueue.getBindings()){
                if(isMatching(bindingOfQueue.getBindingKey(),routingKey)) {
                    sendMessage(message);
                    break;
                }
            }
        } else {
           sendMessage(message);
        }

	}

    private void sendMessage(QueueEntry queueEntry) throws AndesException {
        try {
        AMQProtocolSession session = channel.getProtocolSession();
        ((AMQMessage) queueEntry.getMessage()).setClientIdentifier(session);
        channel.incrementNonAckedMessageCount();
        if (amqpSubscription instanceof SubscriptionImpl.AckSubscription) {
            //this check is needed to detect if subscription has suddenly closed
            if (log.isDebugEnabled()) {
                String msgHeaderStringID = (String)queueEntry.getMessageHeader().
                        getHeader("msgID");
                log.debug("TRACING>> QDW- sent queue/durable topic message " + msgHeaderStringID == null ? "" : msgHeaderStringID + " messageID-" + queueEntry.getMessage().getMessageNumber() + "-to subscription " + amqpSubscription);
            }
            amqpSubscription.send(queueEntry);
        }else{
            throw new AndesException("Unexpected Subscription type");
        }
        } catch (AMQException e) {
            throw new AndesException(e);
        }
    }

    private boolean isMatching(String binding, String topic) {
        boolean isMatching = false;
        if (binding.equals(topic)) {
            isMatching = true;
        } else if (binding.indexOf(".#") > 1) {
            String p = binding.substring(0, binding.indexOf(".#"));
            Pattern pattern = Pattern.compile(p + ".*");
            Matcher matcher = pattern.matcher(topic);
            isMatching = matcher.matches();
        } else if (binding.indexOf(".*") > 1) {
            String p = binding.substring(0, binding.indexOf(".*"));
            Pattern pattern = Pattern.compile("^" + p + "[.][^.]+$");
            Matcher matcher = pattern.matcher(topic);
            isMatching = matcher.matches();
        }
        return isMatching;
    }

	
	public LocalSubscription createQueueToListentoTopic(){
        //todo:hasitha:verify passing null values
		return new AMQPLocalSubscription(amqQueue, 
        		amqpSubscription, subscriptionID, targetQueue, false, isExclusive, true, MessagingEngine.getMyNodeQueueName(),amqQueue.getName(),amqQueue.getOwner().toString(), AMQPUtils.DIRECT_EXCHANGE_NAME,true);
	}
}
