/*
 * Copyright (c) 2016, WSO2 Inc. (http://wso2.com) All Rights Reserved.
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

package org.wso2.andes.kernel.router;


import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.amqp.AMQPUtils;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.ProtocolType;
import org.wso2.andes.kernel.subscription.StorageQueue;


import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Bitmap based topic matcher
 */
public class TopicRoutingMatcher {

    private Log log = LogFactory.getLog(TopicRoutingMatcher.class);

    /**
     * The topic delimiter to differentiate each constituent according to the current protocol type.
     */
    private String constituentsDelimiter;

    /**
     * The multi level matching wildcard according to the current protocol type.
     */
    private String multiLevelWildCard;

    /**
     * The single level matching wildcard according to the current protocol type.
     */
    private String singleLevelWildCard;

    private ProtocolType protocolType;

    // 'Null' and 'Other' constituents are picked from restricted topic characters

    /**
     * Constituent name to represent that a constituent is not available at this location.
     */
    private static final String NULL_CONSTITUENT = "%null%";

    /**
     * Constituent name to represent any constituent except a wildcard.
     */
    private static final String OTHER_CONSTITUENT = "%other%";

    /**
     * Keeps all the storage queues.
     */
    private List<StorageQueue> storageQueueList = new ArrayList<>();

    /**
     * Keeps all the binding keys of storage queues broken into their constituents.
     */
    private Map<Integer, String[]> queueConstituents = new HashMap<>();

    /**
     * Keeps all the constituent tables as ListOfConstituentTables <ConstituentPart, BitSet>.
     */
    private List<Map<String, BitSet>> constituentTables = new ArrayList<>();

    /**
     * Initialize BitMapHandler with the protocol type.
     *
     * @param protocolType The protocol type to handle
     */
    public TopicRoutingMatcher(ProtocolType protocolType) {
        if (ProtocolType.AMQP == protocolType) {
            constituentsDelimiter = ".";
            // AMQPUtils keep wildcard concatenated with constituent delimiter, hence removing them get wildcard only
            multiLevelWildCard = AMQPUtils.TOPIC_AND_CHILDREN_WILDCARD.replace(constituentsDelimiter, "");
            singleLevelWildCard = AMQPUtils.IMMEDIATE_CHILDREN_WILDCARD.replace(constituentsDelimiter, "");
        }  else {
            throw new RuntimeException("Protocol type " + protocolType + " is not recognized.");
        }

        this.protocolType = protocolType;
    }


    public void addStorageQueue(StorageQueue storageQueue) throws AndesException {
        String bindingKey = storageQueue.getMessageRouterBindingKey();

        if (StringUtils.isNotEmpty(bindingKey)) {
            if (!isStorageQueueAvailable(storageQueue)) {
                int newQueueIndex = storageQueueList.size();

                // The index is added to make it clear to which index this is being inserted
                storageQueueList.add(newQueueIndex, storageQueue);

                String constituents[] = bindingKey.split(Pattern.quote(constituentsDelimiter));

                queueConstituents.put(newQueueIndex, constituents);


                for (int constituentIndex = 0; constituentIndex < constituents.length; constituentIndex++) {
                    String constituent = constituents[constituentIndex];

                    Map<String, BitSet> constituentTable;

                    if ((constituentIndex + 1) > constituentTables.size()) {
                        // No tables exist for this constituent index need to create
                        constituentTable = addConstituentTable(constituentIndex);
                    } else {
                        constituentTable = constituentTables.get(constituentIndex);
                    }

                    if (!constituentTable.keySet().contains(constituent)) {
                        // This constituent is not available in this table. Need to add a new row
                        addConstituentRow(constituent, constituentIndex);
                    }
                }

                addStorageQueueColumn(bindingKey, newQueueIndex);
            } else {
                updateStorageQueue(storageQueue);
            }

        } else {
            throw new AndesException("Error adding a new storageQueue. Subscribed bindingKey is empty.");
        }
    }



    public void updateStorageQueue(StorageQueue storageQueue) {
        if (isStorageQueueAvailable(storageQueue)) {
            // Need to add the new entry to the same index since bitmap logic is dependent on this index
            int index = storageQueueList.indexOf(storageQueue);

            // Should not allow to modify this list until the update is complete
            // Otherwise the storageQueue indexes will be invalid
            synchronized (storageQueueList) {
                storageQueueList.remove(index);
                storageQueueList.add(index, storageQueue);
            }
        }
    }

    /**
     * @param constituentIndex The index to create the constituent for
     * @return The created constituent table
     */
    private Map<String, BitSet> addConstituentTable(int constituentIndex) {
        Map<String, BitSet> constituentTable = new HashMap<>();

        BitSet nullBitSet = new BitSet(storageQueueList.size());
        BitSet otherBitSet = new BitSet(storageQueueList.size());

        // Fill null and other constituent values for all available queues
        for (int queueIndex = 0; queueIndex < storageQueueList.size(); queueIndex++) {
            String[] constituentsOfQueue = queueConstituents.get(queueIndex);

            if (constituentsOfQueue.length < constituentIndex + 1) {
                // There is no constituent in this queue for this constituent index
                nullBitSet.set(queueIndex);

                // If last constituent of the queue is multiLevelWildCard, then any other is a match
                if (multiLevelWildCard.equals(constituentsOfQueue[constituentsOfQueue.length - 1])) {
                    otherBitSet.set(queueIndex);
                }
            } else {
                String queueConstituent = constituentsOfQueue[constituentIndex];

                // Check if this is a wildcard
                if (multiLevelWildCard.equals(queueConstituent) ||
                        singleLevelWildCard.equals(queueConstituent)) {
                    otherBitSet.set(queueIndex);
                }
            }
        }

        // Add 'null' and 'other' constituent
        constituentTable.put(NULL_CONSTITUENT, nullBitSet);
        constituentTable.put(OTHER_CONSTITUENT, otherBitSet);

        constituentTables.add(constituentIndex, constituentTable);

        return constituentTable;
    }


    /**
     * Run through each constituentTable and insert a new column for a new storage queue filling it's values
     * by comparing constituents.
     * <p/>
     * This will only fill values for the already available constituents. Will not add new constituents.
     *
     * @param bindingKey The newly subscribed destination
     */
    private void addStorageQueueColumn(String bindingKey, int queueIndex) throws AndesException {

        String[] bindingKeyConstituents = queueConstituents.get(queueIndex);

        // Create a mock destination with two constituents for 'other' wildcard matching
        String matchDestinationForOther = OTHER_CONSTITUENT + constituentsDelimiter + OTHER_CONSTITUENT;

        // Create a mock destination with three constituents for 'null' wildcard matching
        String matchDestinationForNull = NULL_CONSTITUENT + constituentsDelimiter + NULL_CONSTITUENT +
                constituentsDelimiter + NULL_CONSTITUENT;

        // Loop through each constituent table for the new constituents
        for (int constituentIndex = 0; constituentIndex < bindingKeyConstituents.length;
             constituentIndex++) {
            String currentConstituent = bindingKeyConstituents[constituentIndex];
            Map<String, BitSet> constituentTable = constituentTables.get(constituentIndex);

            // Loop through each constituent row in the table and fill values
            for (Map.Entry<String, BitSet> constituentRow : constituentTable.entrySet()) {
                String constituentOfCurrentRow = constituentRow.getKey();
                BitSet bitSet = constituentRow.getValue();

                if (constituentOfCurrentRow.equals(currentConstituent)) {
                    bitSet.set(queueIndex);
                } else if (NULL_CONSTITUENT.equals(constituentOfCurrentRow)) {
                    // Check if this constituent being null matches the destination if we match it with
                    // a null constituent
                    String wildcardDestination = NULL_CONSTITUENT + constituentsDelimiter +
                            currentConstituent;
                    bitSet.set(queueIndex, isMatchForProtocolType(wildcardDestination,
                            matchDestinationForNull));
//                    }
                } else if (OTHER_CONSTITUENT.equals(constituentOfCurrentRow)) {
                    // Check if other is matched by comparing wildcard through specific wildcard matching
                    // Create a mock destinations with current constituent added last and check if it match with a
                    // non-wildcard destination match with the corresponding matching method
                    String wildCardDestination = OTHER_CONSTITUENT + constituentsDelimiter + currentConstituent;

                    bitSet.set(queueIndex, isMatchForProtocolType(wildCardDestination,
                            matchDestinationForOther));
                } else if (singleLevelWildCard.equals(currentConstituent) ||
                        multiLevelWildCard.equals(currentConstituent)) {
                    // If there is any wildcard at this position, then this should match.
                    bitSet.set(queueIndex);
                } else {
                    bitSet.set(queueIndex, false);
                }

            }

        }

        int noOfMaxConstituents = constituentTables.size();

        if (noOfMaxConstituents > bindingKeyConstituents.length) {
            // There are more constituent tables to be filled. Wildcard matching is essential here.

            boolean matchingOthers = true;
            // The OTHER_CONSTITUENT is added here to represent any constituent
            if (!multiLevelWildCard.equals(bindingKeyConstituents[bindingKeyConstituents.length
                    - 1])) {
                String otherConstituentComparer = bindingKey + constituentsDelimiter + OTHER_CONSTITUENT;
                matchingOthers = isMatchForProtocolType(bindingKey, otherConstituentComparer);
            } // Else matchingOthers will be true

            for (int constituentIndex = bindingKeyConstituents.length; constituentIndex <
                    noOfMaxConstituents; constituentIndex++) {
                Map<String, BitSet> constituentTable = constituentTables.get(constituentIndex);

                // Loop through each constituent row in the table and fill values
                for (Map.Entry<String, BitSet> constituentRow : constituentTable.entrySet()) {
                    String constituentOfCurrentRow = constituentRow.getKey();
                    BitSet bitSet = constituentRow.getValue();

                    if (NULL_CONSTITUENT.equals(constituentOfCurrentRow)) {
                        // Null constituent is always true here
                        bitSet.set(queueIndex);
                    } else {
                        bitSet.set(queueIndex, matchingOthers);
                    }
                }
            }
        }

    }

    /**
     * Add a new constituent row for the given constituent index table and fill values for already available
     * queues.
     *
     * @param constituent      The constituent to add
     * @param constituentIndex The index of the constituent
     */
    private void addConstituentRow(String constituent, int constituentIndex) {
        Map<String, BitSet> constituentTable = constituentTables.get(constituentIndex);
        BitSet bitSet = new BitSet();

        for (int i = 0; i < queueConstituents.size(); i++) {
            String[] constituentsOfQueue = queueConstituents.get(i);

            if (constituentIndex < constituentsOfQueue.length) {
                // Get the i'th queue's [constituentIndex]'th constituent
                String queueConstituent = constituentsOfQueue[constituentIndex];
                if (queueConstituent.equals(constituent) || multiLevelWildCard.equals(queueConstituent)
                        || singleLevelWildCard.equals(queueConstituent)) {
                    // The new constituent matches the queues i'th constituent
                    bitSet.set(i);
                } else {
                    // The new constituent does not match the i'th queues [constituentIndex] constituent
                    bitSet.set(i, false);
                }
            } else {
                // The queue does not have a constituent for this index
                // If the last constituent of the queue is multiLevelWildCard we match else false
                if (multiLevelWildCard.equals(constituentsOfQueue[constituentsOfQueue.length - 1])) {
                    bitSet.set(i);
                } else {
                    bitSet.set(i, false);
                }
            }
        }

        constituentTable.put(constituent, bitSet);
    }

    /**
     * Return the match between the given two parameters with respect to the protocol.
     *
     * @param wildCardDestination    The destination with/without wildcard
     * @param nonWildCardDestination The direct destination without wildcards
     * @return Match status
     * @throws AndesException
     */
    private boolean isMatchForProtocolType(String wildCardDestination, String nonWildCardDestination) throws
            AndesException {
        boolean matching = false;

        if (ProtocolType.AMQP == protocolType) {
            matching = AMQPUtils.isTargetQueueBoundByMatchingToRoutingKey(wildCardDestination, nonWildCardDestination);
        } else {
            throw new AndesException("Protocol type " + protocolType + " is not recognized.");
        }

        return matching;
    }

    /**
     * This methods adds a constituent table with only null and other constituents.
     * This is required when a message comes with more than the available number of constituents. If wildcard
     * queues are available for those, they should match. Hence need to create these empty constituent tables.
     */
    private void addEmptyConstituentTable() {
        int noOfqueues = storageQueueList.size();
        Map<String, BitSet> constituentTable = new HashMap<>();

        BitSet nullBitSet = new BitSet(noOfqueues);
        BitSet otherBitSet = new BitSet(noOfqueues);

        if (noOfqueues > 0) {

            // Null constituent will always be true for empty constituents, hence need to flip
            nullBitSet.flip(0, noOfqueues - 1);

            for (int queueIndex = 0; queueIndex < noOfqueues; queueIndex++) {
                // For 'other', if subscribers last constituent is multi level wild card then matching
                String[] allConstituent = queueConstituents.get(queueIndex);
                String lastConstituent = allConstituent[allConstituent.length - 1];

                if (multiLevelWildCard.equals(lastConstituent)) {
                    otherBitSet.set(queueIndex);
                } else {
                    otherBitSet.set(queueIndex, false);
                }
            }
        }

        constituentTable.put(NULL_CONSTITUENT, nullBitSet);
        constituentTable.put(OTHER_CONSTITUENT, otherBitSet);

        constituentTables.add(constituentTable);
    }

    /**
     * Removing a storageQueue from the structure.
     *
     * @param storageQueue The storageQueue to remove
     */
    public void removeStorageQueue(StorageQueue storageQueue) {
        int queueIndex = storageQueueList.indexOf(storageQueue);

        if (queueIndex > -1) {
            for (Map<String, BitSet> constituentTable : constituentTables) {
                for (Map.Entry<String, BitSet> constituentRow : constituentTable.entrySet()) {
                    // For every row create a new BitSet with the values for the removed storageQueue removed
                    String constituent = constituentRow.getKey();
                    BitSet bitSet = constituentRow.getValue();
                    BitSet newBitSet = new BitSet();

                    int bitIndex = 0;

                    for (int i = 0; i < bitSet.size(); i++) {
                        if (bitIndex == queueIndex) {
                            // If the this is the index to remove then skip this round
                            bitIndex++;
                        }
                        newBitSet.set(i, bitSet.get(bitIndex));
                        bitIndex++;
                    }

                    constituentTable.put(constituent, newBitSet);

                }
            }

            // Remove the storageQueue from storageQueue list
            storageQueueList.remove(queueIndex);
        } else {
            log.warn("Storage queue for with name : " + storageQueue.getName() + " is not found to " +
                    "remove");
        }
    }


    public boolean isStorageQueueAvailable(StorageQueue storageQueue) {
        return storageQueueList.contains(storageQueue);
    }


    /**
     * Get storage queues matching to routing key
     * @param routingKey routing key to match queues
     * @return set of storage queues
     */
    public Set<StorageQueue> getMatchingStorageQueues(String routingKey) {
        Set<StorageQueue> matchingQueues = new HashSet<>();

        if (StringUtils.isNotEmpty(routingKey)) {

            // constituentDelimiter is quoted to avoid making the delimiter a regex symbol
            String[] constituents = routingKey.split(Pattern.quote(constituentsDelimiter),-1);

            int noOfCurrentMaxConstituents = constituentTables.size();

            // If given routingKey has more constituents than any subscriber has, then create constituent tables
            // for those before collecting matching subscribers
            if (constituents.length > noOfCurrentMaxConstituents) {
                for (int i = noOfCurrentMaxConstituents; i < constituents.length; i++) {
                    addEmptyConstituentTable();
                }
            }

            // Keeps the results of 'AND' operations between each bit sets
            BitSet andBitSet = new BitSet(storageQueueList.size());

            // Since BitSet is initialized with false for each element we need to flip
            andBitSet.flip(0, storageQueueList.size());

            // Get corresponding bit set for each constituent in the routingKey and operate bitwise AND operation
            for (int constituentIndex = 0; constituentIndex < constituents.length; constituentIndex++) {
                String constituent = constituents[constituentIndex];
                Map<String, BitSet> constituentTable = constituentTables.get(constituentIndex);

                BitSet bitSetForAnd = constituentTable.get(constituent);

                if (null == bitSetForAnd) {
                    // The constituent is not found in the table, hence matching with 'other' constituent
                    bitSetForAnd = constituentTable.get(OTHER_CONSTITUENT);
                }

                andBitSet.and(bitSetForAnd);
            }

            // If there are more constituent tables, get the null constituent in each of them and operate bitwise AND
            for (int constituentIndex = constituents.length; constituentIndex < constituentTables.size();
                 constituentIndex++) {
                Map<String, BitSet> constituentTable = constituentTables.get(constituentIndex);
                andBitSet.and(constituentTable.get(NULL_CONSTITUENT));
            }


            // Valid queues are filtered, need to pick from queue pool
            int nextSetBitIndex = andBitSet.nextSetBit(0);
            while (nextSetBitIndex > -1) {
                matchingQueues.add(storageQueueList.get(nextSetBitIndex));
                nextSetBitIndex = andBitSet.nextSetBit(nextSetBitIndex + 1);
            }

        } else {
            log.warn("Cannot retrieve storage queues via bitmap handler since routingKey to match is empty");
        }

        return matchingQueues;
    }

    /**
     * Get all the storage queues currently saved.
     *
     * @return List of all storage queues
     */
    public List<StorageQueue> getAllStorageQueues() {
        return storageQueueList;
    }


    /**
     * Get all binding keys saved
     *
     * @return set of different binding keys
     */
    public Set<String> getAllBindingKeys() {
        Set<String> topics = new HashSet<>();


        for (Map.Entry<Integer, String[]> subcriberConstituent : queueConstituents.entrySet()) {

            StringBuilder topic = new StringBuilder();
            String[] constituents =  subcriberConstituent.getValue();

            for (int i = 0; i < constituents.length; i++) {
                String constituent = constituents[i];
                // if this is a wildcard constituent, we provide it as 'ANY' in it's place for readability
                if (multiLevelWildCard.equals(constituent) || singleLevelWildCard.equals(constituent)) {
                    topic.append("ANY");
                } else {
                    topic.append(constituent);
                }

                // append the delimiter if there are more constituents to come
                if ((constituents.length - 1) > i) {
                    topic.append(constituentsDelimiter);
                }

            }

            topics.add(topic.toString());
        }

        return topics;
    }

}
