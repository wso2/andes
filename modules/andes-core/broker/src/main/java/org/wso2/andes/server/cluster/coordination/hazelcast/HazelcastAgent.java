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
package org.wso2.andes.server.cluster.coordination.hazelcast;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.IdGenerator;
import com.hazelcast.core.Member;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.server.cluster.coordination.CoordinationConstants;
import org.wso2.andes.server.cluster.coordination.SubscriptionNotification;

import java.util.*;

/**
 * This is a singleton class, which contains all Hazelcast related operations.
 */
public class HazelcastAgent {
    private static Log log = LogFactory.getLog(HazelcastAgent.class);

    private static HazelcastAgent hazelcastAgent;

    private HazelcastInstance hazelcastInstance;
    private ITopic subscriptionChangedNotifierChannel;
    private ITopic queueChangedNotifierChannel;
    private int uniqueIdOfLocalMember;

    private HazelcastAgent(){
        log.info("Initializing Hazelcast Agent");
        this.hazelcastInstance = AndesContext.getInstance().getHazelcastInstance();
        this.hazelcastInstance.getCluster().addMembershipListener(new AndesMembershipListener());

        this.subscriptionChangedNotifierChannel = this.hazelcastInstance.getTopic(
                CoordinationConstants.HAZELCAST_SUBSCRIPTION_CHANGED_NOTIFIER_TOPIC_NAME);
        this.subscriptionChangedNotifierChannel.addMessageListener(new SubscriptionChangedListener());

        this.queueChangedNotifierChannel = this.hazelcastInstance.getTopic(
                CoordinationConstants.HAZELCAST_QUEUE_CHANGED_NOTIFIER_TOPIC_NAME);
        this.queueChangedNotifierChannel.addMessageListener(new QueueChangedListener());

        IdGenerator idGenerator =hazelcastInstance.getIdGenerator(CoordinationConstants.HAZELCAST_ID_GENERATOR_NAME);
        this.uniqueIdOfLocalMember = (int) idGenerator.newId();
        log.info("Successfully initialized Hazelcast Agent");
        log.info("Unique ID generation for message ID generation:" + uniqueIdOfLocalMember);
    }

    public static HazelcastAgent getInstance() {
        if (hazelcastAgent == null) {
            synchronized (HazelcastAgent.class) {
                if(hazelcastAgent == null)  {
                    hazelcastAgent = new HazelcastAgent();
                }
            }
        }

        return hazelcastAgent;
    }

    /**
     * Node ID is generated in the format of "NODE/<host IP>_<Node UUID>"
     * @return NodeId
     */
    public String getNodeId(){
        Member localMember = hazelcastInstance.getCluster().getLocalMember();
        return CoordinationConstants.NODE_NAME_PREFIX
                + localMember.getInetSocketAddress().getAddress()
                + "_"
                + localMember.getUuid() ;
    }

    /**
     * All members of the cluster are returned as a Set
     * @return Set of Member
     */
    public Set<Member> getAllClusterMembers(){
        return hazelcastInstance.getCluster().getMembers();
    }


    public List<String> getMembersNodeIDs(){
        Set<Member> members = this.getAllClusterMembers();
        List<String> nodeIDList = new ArrayList<String>();
        for(Member member: members){
            nodeIDList.add(CoordinationConstants.NODE_NAME_PREFIX
                    + member.getInetSocketAddress().getAddress()
                    + "_"
                    + member.getUuid());
        }

        return nodeIDList;
    }

    public Member getLocalMember(){
        return hazelcastInstance.getCluster().getLocalMember();
    }

    public int getClusterSize(){
        return hazelcastInstance.getCluster().getMembers().size();
    }

    public int getUniqueIdForTheNode(){
        return uniqueIdOfLocalMember;
    }

    public String getIdOfNode(Member node){
        return CoordinationConstants.NODE_NAME_PREFIX
                + node.getInetSocketAddress().getAddress()
                + "_"
                + node.getUuid() ;
    }

    public int getIndexOfNode(Member node){
        List<String> membersUniqueRepresentations = new ArrayList<String>();
        for(Member member: this.getAllClusterMembers()){
            membersUniqueRepresentations.add(member.getUuid());
        }
        Collections.sort(membersUniqueRepresentations);
        int indexOfMyId = membersUniqueRepresentations.indexOf(node.getUuid());
        return indexOfMyId;
    }

    public int getIndexOfLocalNode(){
        return this.getIndexOfNode(this.getLocalMember());
    }

    public void notifySubscriberChanged(SubscriptionNotification subscriptionNotification){
        log.info("Handling cluster gossip: Sending subscriber changed notification to cluster...");
        this.subscriptionChangedNotifierChannel.publish(subscriptionNotification);
    }
}
