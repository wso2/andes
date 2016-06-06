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

import org.apache.log4j.Logger;
import org.python.antlr.op.In;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;

import java.util.HashMap;

/**
 * Created by sasikala on 6/6/16.
 */
public class MasterSlaveInfo {

    private static boolean isMaster;
    private static String nodeId;

    public static MasterSlaveInfo masterSlaveInfo;

    public MasterSlaveInfo(){
        isMaster = false;
    }

    static {
        masterSlaveInfo = new MasterSlaveInfo();
    }

    public static MasterSlaveInfo getMasterSlaveInfo(){
        return masterSlaveInfo;
    }

    public boolean isMaster(){
        return isMaster;
    }

    public String getNodeId(){
        return nodeId;
    }

    public void setMaster(boolean master) {
        isMaster = master;
        if (master) {
            System.out.println("This node is elected as the master. node id: " + nodeId);
        }
        else{
            System.out.println("This node is elected as the slave. node id: " + nodeId);
        }
    }
}
