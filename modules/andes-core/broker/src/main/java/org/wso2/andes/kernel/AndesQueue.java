/*
*  Copyright (c) 2005-2010, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
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

package org.wso2.andes.kernel;

import org.apache.commons.lang.builder.HashCodeBuilder;

public class AndesQueue {
    public String queueName;
    public String queueOwner;
    public boolean isExclusive;
    public boolean isDurable;
    public int subscriptionCount;

    /**
     * create an instance of andes queue
     *
     * @param queueName   name of the queue
     * @param queueOwner  owner of the queue (virtualhost)
     * @param isExclusive is queue exclusive
     * @param isDurable   is queue durable
     */
    public AndesQueue(String queueName, String queueOwner, boolean isExclusive, boolean isDurable) {
        this.queueName = queueName;
        this.queueOwner = queueOwner;
        this.isExclusive = isExclusive;
        this.isDurable = isDurable;
        this.subscriptionCount = 1;
    }

    /**
     * create an instance of andes queue
     *
     * @param queueAsStr queue information as encoded string
     */
    public AndesQueue(String queueAsStr) {
        String[] propertyToken = queueAsStr.split(",");
        for (String pt : propertyToken) {
            String[] tokens = pt.split("=");
            if (tokens[0].equals("queueName")) {
                this.queueName = tokens[1];
            } else if (tokens[0].equals("isExclusive")) {
                this.queueOwner = tokens[1].equals("null") ? null : tokens[1];
            } else if (tokens[0].equals("isExclusive")) {
                this.isExclusive = Boolean.parseBoolean(tokens[1]);
            } else if (tokens[0].equals("isDurable")) {
                this.isDurable = Boolean.parseBoolean(tokens[1]);
            }
        }
    }

    public String toString() {
        StringBuffer buf = new StringBuffer();
        buf.append("[").append(queueName)
                .append("] OW=").append(queueOwner)
                .append("/X=").append(isExclusive)
                .append("/D").append(isDurable);
        return buf.toString();
    }

    public String encodeAsString() {
        StringBuffer buf = new StringBuffer();
        buf.append("queueName=").append(queueName)
                .append(",queueOwner=").append(queueOwner)
                .append(",isExclusive=").append(isExclusive)
                .append(",isDurable=").append(isDurable);
        return buf.toString();
    }

    public boolean equals(Object o) {
        if (o instanceof AndesQueue) {
            AndesQueue c = (AndesQueue) o;
            if (this.queueName.equals(c.queueName)) {
                return true;
            }
        }
        return false;
    }

    public int hashCode() {
        return new HashCodeBuilder(17, 31).
                append(queueName).
                toHashCode();
    }
}
