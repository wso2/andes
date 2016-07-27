/*
 * Copyright (c) 2016 WSO2 Inc. (http://wso2.com) All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wso2.andes.client;

import org.wso2.andes.client.message.AbstractJMSMessage;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * Represents objects that can be inserted into {@link BasicMessageConsumer#_synchronousQueue}.
 */
public class DelayedObject implements Delayed {
    /**
     * Time at which the delayed object is created. This is required to make the calculation of the delay when popped
     * from the queue.
     */
    private final long startTime;

    /**
     * The object value.
     */
    private final Object objectElement;

    /**
     * Creates a new delayed object
     *
     * @param deliveryDelay The delay at which the object should be held upon before releasing.
     *                      Value is in milliseconds.
     * @param objectElement The object value to be kept stored in this container.
     */
    public DelayedObject(long deliveryDelay, Object objectElement) {
        this.startTime = System.currentTimeMillis() + deliveryDelay;
        this.objectElement = objectElement;
    }

    /**
     * Gets the object stored in this container.
     *
     * @return The object.
     */
    public Object getObject() {
        return objectElement;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getDelay(TimeUnit timeUnit) {
        long diff = startTime - System.currentTimeMillis();
        return timeUnit.convert(diff, TimeUnit.MILLISECONDS);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int compareTo(Delayed delayedObject) {
        int compareToValue = Long.compare(this.startTime, ((DelayedObject) delayedObject).startTime);
        DelayedObject compareToDelayedObject = (DelayedObject) delayedObject;
        Object compareToObject = compareToDelayedObject.getObject();
        if (0 == compareToValue && objectElement instanceof AbstractJMSMessage && compareToObject instanceof AbstractJMSMessage) {
            AbstractJMSMessage currentAbstractJMSMessage = (AbstractJMSMessage) this.objectElement;
            AbstractJMSMessage compareToAbstractJMSMessage = (AbstractJMSMessage) compareToObject;
            compareToValue = Long.compare(currentAbstractJMSMessage.getDeliveryTag(), compareToAbstractJMSMessage.getDeliveryTag());
        }

        return compareToValue;
    }
}