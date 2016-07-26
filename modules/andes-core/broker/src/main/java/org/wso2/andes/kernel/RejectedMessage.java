/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.andes.kernel;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * A wrapper class for rejected messages which supports delayed delivery.
 */
public class RejectedMessage implements Delayed {
    /**
     * Time at which the rejected message was created. This is required to make the calculation of the delay when popped
     * from the queue.
     */
    private final long timeOfRejectedMessageCreated;

    /**
     * The delay to be hold by the broker.
     */
    private final long deliveryDelay;

    /**
     * The andes message metadata.
     */
    private final DeliverableAndesMetadata deliverableAndesMetadata;

    /**
     * Created new instance of a rejected message.
     *
     * @param deliveryDelay            The delay at which the rejected message should be held upon.
     * @param deliverableAndesMetadata The message metadata.
     */
    public RejectedMessage(long deliveryDelay, DeliverableAndesMetadata deliverableAndesMetadata) {
        this.timeOfRejectedMessageCreated = System.currentTimeMillis();
        this.deliveryDelay = deliveryDelay;
        this.deliverableAndesMetadata = deliverableAndesMetadata;
    }

    public DeliverableAndesMetadata getDeliverableAndesMetadata() {
        return deliverableAndesMetadata;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getDelay(TimeUnit timeUnit) {
        return timeUnit.convert(deliveryDelay - (System.currentTimeMillis() - timeOfRejectedMessageCreated),
                TimeUnit.MILLISECONDS);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int compareTo(Delayed rejectedMessage) {
        if (this == rejectedMessage) {
            return 0;
        }

        if (rejectedMessage instanceof RejectedMessage) {
            return new Long(deliveryDelay - ((RejectedMessage) rejectedMessage).deliveryDelay).intValue();
        }

        return new Long(getDelay(TimeUnit.MILLISECONDS) - rejectedMessage.getDelay(TimeUnit.MILLISECONDS)).intValue();
    }
}
