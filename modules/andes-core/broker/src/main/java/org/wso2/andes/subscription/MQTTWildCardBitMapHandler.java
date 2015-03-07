/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *   WSO2 Inc. licenses this file to you under the Apache License,
 *   Version 2.0 (the "License"); you may not use this file except
 *   in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an
 *   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *   KIND, either express or implied.  See the License for the
 *   specific language governing permissions and limitations
 *   under the License.
 */

package org.wso2.andes.subscription;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.AndesSubscription;
import org.wso2.andes.mqtt.MQTTUtils;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MQTTWildCardBitMapHandler {

    private static final Log log = LogFactory.getLog(MQTTUtils.class);

    private static final String CONSTITUENTS_DELIMITER = "/";

    // Null and Other constituents are picked from restricted topic characters
    private static final String NULL_CONSTITUENT = "%null%";
    private static final String OTHER_CONSTITUENT = "%other%";

    /**
     * Keeps all the wildcard subscriptions indexed as <SubscriptionIndex, Subscription>.
      */
    private List<AndesSubscription> wildCardSubscriptionMap = new ArrayList<AndesSubscription>();

    /**
     * Keeps all the subscription destinations broken into their constituents.
     */
    private Map<Integer, String[]> subscriptionConstituents = new HashMap<Integer, String[]>();

    /**
     * Keeps all the constituent tables as ListOfConstituentTables <ConstituentPart, BitSet>.
      */
    private List<Map<String, BitSet>> constituentTables = new ArrayList<Map<String, BitSet>>();

    public void addWildCardSubscription(AndesSubscription subscription) {
        String destination = subscription.getSubscribedDestination();

        if (StringUtils.isNotEmpty(destination)) {
            int newSubscriptionIndex = wildCardSubscriptionMap.size();

            // The index is added to make it clear to which index this is being inserted
            wildCardSubscriptionMap.add(newSubscriptionIndex, subscription);

            String constituents[] = destination.split(CONSTITUENTS_DELIMITER);


            for (int constituentIndex = 0; constituentIndex < constituents.length; constituentIndex++) {
                String constituent = constituents[constituentIndex];
                Map<String, BitSet> constituentTable = constituentTables.get(constituentIndex);

                // If there is no table for this constituent number then create it
                if (null == constituentTable) {
                    constituentTable = new HashMap<String, BitSet>();

                    // Add 'null' and 'other' constituent
                    constituentTable.put(NULL_CONSTITUENT, new BitSet(wildCardSubscriptionMap.size()));
                    constituentTable.put(OTHER_CONSTITUENT, new BitSet(wildCardSubscriptionMap.size()));

                    constituentTables.add(constituentIndex, constituentTable);
                }

                if (!constituentTable.keySet().contains(constituent)) {
                    // This constituent is not available in this table. Need to add a new row
                    addConstituentRow(constituent, constituentIndex);
                }
            }

            addSubscriptionColumn(destination, newSubscriptionIndex);

        } else {
            log.error("Subscription is empty");
            //TODO:Throw
        }
    }


    /**
     * Run through each constituentTable and insert a new column for a new subscription filling it's values
     * by comparing constituents.
     *
     * This will only fill values for the already available constituents. Will not add new constituents.
     *
     * @param subscribedDestination The newly subscribed destination
     */
    private void addSubscriptionColumn(String subscribedDestination, int subscriptionIndex) {

        String[] subscribedDestinationConstituents = subscribedDestination.split(CONSTITUENTS_DELIMITER);

        // Loop through each constituent table for the new constituents
        for (int constituentIndex = 0; constituentIndex < subscribedDestinationConstituents.length; constituentIndex++) {
            String currentConstituent = subscribedDestinationConstituents[constituentIndex];
            Map<String, BitSet> constituentTable = constituentTables.get(constituentIndex);

            // Loop through each constituent row in the table and fill values
            for (Map.Entry<String, BitSet> constituentRow : constituentTable.entrySet()) {
                String constituentOfCurrentRow = constituentRow.getKey();
                BitSet bitSet = constituentRow.getValue();

                if (constituentOfCurrentRow.equals(currentConstituent)) {
                    bitSet.set(subscriptionIndex);
                } else if (NULL_CONSTITUENT.equals(constituentOfCurrentRow)) {
                    int noOfSubscriptions = wildCardSubscriptionMap.size();
                    BitSet nullBitSet = new BitSet(noOfSubscriptions);
                    // Set all bits after the number of constituents to true
                    for (int i = subscribedDestinationConstituents.length; i < noOfSubscriptions; i++) {
                        nullBitSet.set(i);
                    }
                } else if (OTHER_CONSTITUENT.equals(constituentOfCurrentRow)) {
                    // Check if other is matched by comparing wildcard through specific wildcard matching
                    MQTTUtils.isTargetQueueBoundByMatchingToRoutingKey(currentConstituent, constituentOfCurrentRow);
                } else {
                    bitSet.set(subscriptionIndex, false);
                }

            }
        }

        int noOfMaxConstituents = constituentTables.size();

        if (noOfMaxConstituents > subscribedDestinationConstituents.length) {
            // There are more constituent tables to be filled. Wildcard matching is essential here

            // The OTHER_CONSTITUENT is added here to represent any constituent here
            String otherConstituentComparer = subscribedDestination + CONSTITUENTS_DELIMITER + OTHER_CONSTITUENT;
            boolean matchingOthers = MQTTUtils.isTargetQueueBoundByMatchingToRoutingKey(subscribedDestination, otherConstituentComparer);

            for (int constituentIndex = subscribedDestinationConstituents.length; constituentIndex < noOfMaxConstituents; constituentIndex++) {
                Map<String, BitSet> constituentTable = constituentTables.get(constituentIndex);

                // Loop through each constituent row in the table and fill values
                for (Map.Entry<String, BitSet> constituentRow : constituentTable.entrySet()) {
                    String constituentOfCurrentRow = constituentRow.getKey();
                    BitSet bitSet = constituentRow.getValue();

                    if (NULL_CONSTITUENT.equals(constituentOfCurrentRow)) {
                        // Null constituent is always true here
                        bitSet.set(subscriptionIndex);
                    } else {
                        bitSet.set(subscriptionIndex, matchingOthers);
                    }
                }
            }
        }

    }

    private void addConstituentRow(String constituent, int constituentIndex) {
        Map<String, BitSet> constituentTable = constituentTables.get(constituentIndex);
        BitSet bitSet = new BitSet();

        // Create a destination with only the constituent and wildcards in the front
        // Eg :- If constituent is 'mb' is the 3rd constituent, we create a string as '*/*/mb'
//        StringBuilder constituentDestination = new StringBuilder(MQTTUtils.MULTI_LEVEL_WILDCARD);
//
//        for (int i = 0; i < constituentIndex - 2; i++) {
//            constituentDestination.append(CONSTITUENTS_DELIMITER).append(MQTTUtils.MULTI_LEVEL_WILDCARD);
//        }
//
//        constituentDestination.append(CONSTITUENTS_DELIMITER).append(constituent);

        for (int i = 0; i < subscriptionConstituents.size(); i++) {
            if (subscriptionConstituents.get(i).equals(constituent)) {
                // The new constituent matches the subscriptions i'th constituent
                bitSet.set(i);
            } else {
                // The new constituent does not match the subscriptions i'th constituent
                bitSet.set(i, false);
            }
        }

        constituentTable.put(constituent, bitSet);
    }

    public void removeWildCardSubscription(AndesSubscription subscription) {
        int subscriptionIndex = wildCardSubscriptionMap.indexOf(subscription);

        // TODO:Make this faster. This is too slow even for remove
        // TODO:Also if a row is removed it is also not removed here

        if (subscriptionIndex > -1) {
            for (Map<String, BitSet> constituentTable : constituentTables) {
                for (Map.Entry<String, BitSet> constituentRow : constituentTable.entrySet()) {
                    // For every row create a new BitSet with the values for the removed subscription removed
                    String constituent = constituentRow.getKey();
                    BitSet bitSet = constituentRow.getValue();
                    BitSet newBitSet = new BitSet();

                    int bitIndex = 0;

                    for (int i = 0; i < bitSet.size(); i++) {
                        if (bitIndex == i) {
                            // If the this is the index to remove then skip this round
                            bitIndex++;
                        }
                        newBitSet.set(i, bitSet.get(bitIndex));
                        bitIndex++;
                    }

                    constituentTable.put(constituent, newBitSet);

                }
            }
        } else {
            log.warn("Subscription is not found to remove");
            // TODO:Warn
        }
    }

    public void updateWildCardSubscription(AndesSubscription updatedSubscription) {
        int index = wildCardSubscriptionMap.indexOf(updatedSubscription);

        if (index > -1) {
            // Need to preserve order, hence adding to the same index
            wildCardSubscriptionMap.remove(index);
            wildCardSubscriptionMap.add(index, updatedSubscription);
        } else {
            log.warn("Subscription is not found to update");
            // TODO:Warn
        }
    }

    public Set<AndesSubscription> getMatchingSubscriptions(String destination) {
        Set<AndesSubscription> subscriptions= new HashSet<AndesSubscription>();

        if (StringUtils.isNotEmpty(destination)) {

            String[] constituents = destination.split(CONSTITUENTS_DELIMITER);

            // Keeps the results of 'AND' operations between each bit sets
            BitSet andBitSet = new BitSet(constituents.length);

            // Since BitSet is initialized with false for each element we need to flip
            andBitSet.flip(0, constituents.length - 1);

            for (int constituentIndex = 0; constituentIndex < constituents.length; constituentIndex++) {
                String constituent = constituents[constituentIndex];
                Map<String, BitSet> constituentTable = constituentTables.get(constituentIndex);

                andBitSet.and(constituentTable.get(constituent));
            }

            // Valid subscriptions are not filtered, need to pick from subscription pool

            for (int subscriptionIndex = 0; subscriptionIndex < wildCardSubscriptionMap.size(); subscriptionIndex++) {
                // If set, then pick
                if (andBitSet.get(subscriptionIndex)) {
                    subscriptions.add(wildCardSubscriptionMap.get(subscriptionIndex));
                }
            }

        } else {
            log.warn("Subscription destination is empty");
            // TODO:Warn
        }

        return subscriptions;
    }


}
