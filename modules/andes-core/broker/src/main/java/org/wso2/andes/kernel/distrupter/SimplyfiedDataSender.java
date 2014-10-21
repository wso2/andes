/*
 * Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.andes.kernel.distrupter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.AndesSubscription;
import org.wso2.andes.server.queue.QueueEntry;
import org.wso2.andes.server.subscription.Subscription;
import org.wso2.andes.server.subscription.SubscriptionImpl;

import java.util.List;

public class SimplyfiedDataSender implements AndesSubscription {
    private static Log log = LogFactory.getLog(SimplyfiedDataSender.class);

    public void sendAsynchronouslyToQueueEndPoint(final List<SubscriptionDataEvent> messageList){
        Subscription subscription;
        QueueEntry message;
        for (SubscriptionDataEvent subscriptionDataEvent : messageList) {
            try {
                subscription = subscriptionDataEvent.subscription;
                message = subscriptionDataEvent.message;
                if (subscription instanceof SubscriptionImpl.AckSubscription) {
                    subscription.send(message);
                } else {
                    log.error("Unexpected Subscription Implementation : " +
                            subscription != null ? subscription.getClass().getName() : null);
                }
            } catch (Throwable e) {
                log.error("Error while delivering message ", e);
            }
        }
    }

	@Override
	public String getSubscriptionID() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getSubscribedDestination() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isBoundToTopic() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isDurable() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public String getSubscribedNode() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isExclusive() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void setExclusive(boolean isExclusive) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String encodeAsStr() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getTargetQueue() {
		// TODO Auto-generated method stub
		return null;
	}

    @Override
    public String getTargetQueueOwner() {
        return null;
    }

    @Override
    public String getTargetQueueBoundExchangeName() {
        return null;
    }

    @Override
    public String getTargetQueueBoundExchangeType() {
        return null;
    }

    @Override
    public Short ifTargetQueueBoundExchangeAutoDeletable() {
        return null;
    }

    @Override
    public boolean hasExternalSubscriptions() {
        return false;
    }


}
