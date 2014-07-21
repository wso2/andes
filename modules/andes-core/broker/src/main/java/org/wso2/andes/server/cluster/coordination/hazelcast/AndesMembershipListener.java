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

import com.hazelcast.core.Member;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.cluster.ClusterManager;

public class AndesMembershipListener implements MembershipListener {
    private static Log log = LogFactory.getLog(AndesMembershipListener.class);

    @Override
    public void memberAdded(MembershipEvent membershipEvent) {
        Member member = membershipEvent.getMember();
        log.info("New member joined to the cluster. Member Socket Address:"
                + member.getInetSocketAddress()
                + " UUID:"
                + member.getUuid());
        ClusterResourceHolder.getInstance().getClusterManager().handleNewNodeJoiningToCluster(member);
    }

    @Override
    public void memberRemoved(MembershipEvent membershipEvent) {
        Member member = membershipEvent.getMember();
        log.info("A member left the cluster. Member Socket Address:"
                + member.getInetSocketAddress()
                + " UUID:"
                + member.getUuid());
        ClusterManager clusterManager = ClusterResourceHolder.getInstance().getClusterManager();
        try {
            clusterManager.handleNodeLeavingCluster(member);
        } catch (Exception e) {
            log.error("Error while handling node removal, NodeID:" + clusterManager.getNodeId(member), e);
            e.printStackTrace();
        }
    }
}
