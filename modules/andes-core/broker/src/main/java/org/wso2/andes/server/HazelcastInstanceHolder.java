package org.wso2.andes.server;

import com.hazelcast.core.HazelcastInstance;

public class HazelcastInstanceHolder {
    private static HazelcastInstance hazelcastInstance;

    public static void setHazelcastInstance(HazelcastInstance hazelcastInstance1){
        hazelcastInstance = hazelcastInstance1;
    }

    public static HazelcastInstance getHazelcastInstance(){
        return hazelcastInstance;
    }
}
