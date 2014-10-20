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
package org.wso2.andes.server.logging.subjects;

import org.wso2.andes.server.subscription.Subscription;

import java.text.MessageFormat;

import static org.wso2.andes.server.logging.subjects.LogSubjectFormat.SUBSCRIPTION_FORMAT;

public class SubscriptionLogSubject extends AbstractLogSubject
{

    /**
     * Create an QueueLogSubject that Logs in the following format.
     *
     * @param subscription
     */
    public SubscriptionLogSubject(Subscription subscription)
    {
        // Delegate the formating of the Queue to the QueueLogSubject. So final
        // log string format is:
        // [ sub:<id>(vh(<vhost>)/qu(<queue>)) ]

        String queueString = new QueueLogSubject(subscription.getQueue()).toLogString();

        _logString = "[" + MessageFormat.format(SUBSCRIPTION_FORMAT,
                                                subscription.getSubscriptionID())
                     + "("
                     // queueString is [vh(/{0})/qu({1}) ] so need to trim
                     //                ^                ^^
                     + queueString.substring(1,queueString.length() - 3)
                     + ")"
                     + "] ";

    }
}
