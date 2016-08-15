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
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.wso2.andes.server.cluster.coordination.rdbms;

/**
 * Event that contains information regarding a membership change
 */
public class MembershipEvent {

    /**
     * The node Id for which the membership event was triggered.
     */
    private String member;

    /**
     * Membership event type.
     */
    private MembershipEventType type;

    public MembershipEvent(MembershipEventType type, String member) {
        this.type = type;
        this.member = member;
    }

    /**
     * Retrieve the node id of the member for which the membership event is triggered.
     *
     * @return node id of the member
     */
    public String getMember() {
        return member;
    }

    /**
     * Retrieve the type of the membership event.
     *
     * @return membership event type.
     */
    public MembershipEventType getMembershipEventType() {
        return type;
    }
}
