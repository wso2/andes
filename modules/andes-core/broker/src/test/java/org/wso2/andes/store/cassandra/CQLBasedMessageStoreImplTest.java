package org.wso2.andes.store.cassandra;


import com.datastax.driver.core.Cluster;
import org.junit.Before;
import org.junit.Test;
import org.wso2.andes.server.store.util.CQLDataAccessHelper;

import java.util.ArrayList;
import java.util.List;

public class CQLBasedMessageStoreImplTest {
    private static Cluster cluster;

    @Before
    public static void BeforeClass() throws Exception {
        List<String> hosts = new ArrayList<String>();
        hosts.add("127.0.0.1");
        CQLDataAccessHelper.ClusterConfiguration clusterConfig = new CQLDataAccessHelper
                .ClusterConfiguration("admin", "admin", "TestCluster", hosts, 9042);

        cluster = CQLDataAccessHelper.createCluster(clusterConfig);
        CQLConnection cqlConnection = new CQLConnection();
        cqlConnection.setCluster(cluster);
    }


}
