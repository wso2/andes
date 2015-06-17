package org.wso2.andes.store.cassandra;

import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.QueryResult;

import org.apache.log4j.Logger;


/**
 * Contains utility methods required for both {@link HectorBasedMessageStoreImpl}
 * and {@link HectorBasedAndesContextStoreImpl}
 */
public class HectorUtils {

    private static final Logger log = Logger.getLogger(HectorUtils.class);

    
    /**
     * Tests whether data can be inserted to cluster with configured ( in hector
     * data source) write
     * consistency level.
     * 
     * @param hectorConnection
     *            the connection
     * @param testString
     *            test data
     * @param testTime
     *            test data
     * @return true of insertion succeeds
     */
    boolean testInsert(HectorConnection hectorConnection, String testString, long testTime) {

        boolean canInsert = false;

        try {
            Mutator<String> mutator = HFactory.createMutator(hectorConnection.getKeySpace(), StringSerializer.get());

            HColumn<String, Long> testTimeColumn =
                                                   HFactory.createColumn(HectorConstants.MESSAGE_STORE_STATUS_COLUMN_TIME_STAMP,
                                                                         testTime,
                                                                         StringSerializer.get(), LongSerializer.get());

            mutator.addInsertion(testString, HectorConstants.MESSAGE_STORE_STATUS_COLUMN_FAMILY,
                                 testTimeColumn);
            mutator.execute();
            canInsert = true;
        } catch (HectorException ignore) {
            log.error("unable to insert data", ignore);
        }
        return canInsert;
    }

    
    /**
     * Tests whether data can be read from the cluster with configured ( in
     * hector
     * data source) read
     * consistency level.
     * 
     * @param hectorConnection
     *            the connection
     * @param testString
     *            test data
     * @param testTime
     *            test data
     * @return true of data retrieval succeeds
     */    
    boolean testRead(HectorConnection hectorConnection, String testString, long testTime) {

        boolean canRead = false;

        try {
            

            ColumnQuery<String, String, Long> columnQuery =
                                                            HFactory.createColumnQuery(hectorConnection.getKeySpace(),
                                                                                       StringSerializer.get(),
                                                                                       StringSerializer.get(),
                                                                                       LongSerializer.get());
            
            columnQuery.setColumnFamily(HectorConstants.MESSAGE_STORE_STATUS_COLUMN_FAMILY);
            columnQuery.setKey(testString);
            columnQuery.setName(HectorConstants.MESSAGE_STORE_STATUS_COLUMN_TIME_STAMP);
            
            QueryResult<HColumn<String, Long>> result =  columnQuery.execute();
            
            canRead = result.get() != null;

        } catch (HectorException ignore) {
            log.error("unable to insert data", ignore);
        }
        return canRead;
    }

    /**
     * Tests whether data can be deleted from the cluster with configured ( in
     * hector
     * data source) write
     * consistency level.
     * 
     * @param hectorConnection
     *            the connection
     * @param testString
     *            test data
     * @param testTime
     *            test data
     * @return true of data deletion succeeds
     */
    boolean testDelete(HectorConnection hectorConnection, String testString, long testTime) {

        boolean canDelete = false;

        try {
            Mutator<String> mutator = HFactory.createMutator(hectorConnection.getKeySpace(), StringSerializer.get());

            mutator.addDeletion(testString, HectorConstants.MESSAGE_STORE_STATUS_COLUMN_FAMILY);

            mutator.execute();
            canDelete = true;
        } catch (HectorException ignore) {
            log.error("unable to insert data", ignore);
        }
        return canDelete;
    }


}
