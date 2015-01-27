/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.andes.subscription;

import org.wso2.andes.kernel.AndesSubscription;
import org.wso2.andes.kernel.LocalSubscription;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.BitSet;

/**
 * This class is responsible for adding of the subscriptions into the bitmap, matching and the removal
 * of subscriptions from bitmap
 */
public class SubscriptionBitMapHandler {

    private static final String CONSTITUENT_TOPIC_CONSTANT = "other";
    private final String SPECIAL_CHARACTER_FOR_NULL = "*";
    /**
     * Represents the mapping from criteria to clustered subscriptions
     */
    protected ArrayList<Map<String, java.util.BitSet>> bitMapClustered;
    /**
     * Represents the mapping from criteria to local subscriptions
     * <p/>
     * ArrayList of Bitmap tables
     * <p/>
     * Each bit map table Map<Constituent part, BitMap for the constituent part >
     */
    private ArrayList<Map<String, java.util.BitSet>> bitMapLocal;
    /**
     * Keeps track of the local subscriptions
     * <p/>
     * Map <Index of the subscriber, Map<Subscription ID, LocalSubscription>
     */
    private Map<Integer, Map<String, LocalSubscription>> localSubscriptions;
    /**
     * Keeps track of the Andes subscriptions
     * <p/>
     * Map <Index of the subscriber, Map<Subscription ID, AndesSubscription
     * Ex Suppose if we have three subscribers WSO2, WSO2.#, WSO2.MB
     * Then the subscribers will get the indice 0, 1, 2
     */

    private Map<Integer, Map<String, AndesSubscription>> clusteredSubscriptions;
    /**
     * Mapping from the routing key to the integer
     * <p/>
     * Map<routing key, index>
     * Example : Subscriptions WSO2, WSO2.*, WSO2.MB
     * The index of the Subscriptions will be 0,1,and 2
     * <p/>
     * The Mapping is as follows
     * WSO2 --------> 0
     * WSO2.* -------> 1
     * WSO2.MB -------> 2
     */
    private Map<String, Integer> localSubscriptionMapping;
    private Map<String, Integer> clusteredSubscriptionMapping;
    /**
     * Keeps track of the local subscribers count with distinct routing keys
     */
    private int localSubscriptionCount;
    /**
     * Keeps track of the andes subscribers count with distinct routing keys
     */
    private int clusteredSubscriptionCount;
    /**
     * Keeps track of the deleted local subscription indexes
     * Whenever this arraylist is not empty, the index for the new subscription is found from this
     */
    private ArrayList<Integer> deletedLocals;
    /**
     * Keeps track of the deleted clustered subscription indexes
     */
    private ArrayList<Integer> deletedClusters;
    /**
     * Initialize the BitMap
     */
    public SubscriptionBitMapHandler() {
        localSubscriptions = new ConcurrentHashMap<Integer, Map<String, LocalSubscription>>();
        clusteredSubscriptions = new ConcurrentHashMap<Integer, Map<String, AndesSubscription>>();
        localSubscriptionMapping = new ConcurrentHashMap<String, Integer>();
        clusteredSubscriptionMapping = new ConcurrentHashMap<String, Integer>();
        bitMapLocal = new ArrayList<Map<String, java.util.BitSet>>();
        bitMapClustered = new ArrayList<Map<String, java.util.BitSet>>();
        localSubscriptionCount = 0;
        clusteredSubscriptionCount = 0;
        deletedClusters = new ArrayList<Integer>();
        deletedLocals = new ArrayList<Integer>();
    }

    /**
     * Method to add the LocalSubscription
     *
     * @param destination routing key of the LocalSubscription
     * @param local       LocalSubscription to be added
     */
    public void addLocalSubscription(String destination, LocalSubscription local) {
        int columnIndexOfTheSubscriptionInBitMap;

        /**
         * If there is no any previous subscriptions with the specified routing key
         */
        if (null == localSubscriptionMapping.get(destination)) {
            /**
             * If the removed subscription places still remain take the index for the subscription
             * from there
             */
            if (deletedLocals.size() > 0) {
                columnIndexOfTheSubscriptionInBitMap = deletedLocals.remove(0);
            } else {
                /**
                 * else get the index from the localSubscriptionCount variable
                 */
                columnIndexOfTheSubscriptionInBitMap = localSubscriptionCount;
                localSubscriptionCount++;
            }

            /**
             * Put the routing key and index
             * Mapping from the routing key to index
             */
            localSubscriptionMapping.put(destination, columnIndexOfTheSubscriptionInBitMap);
            /**
             * create the entry at the index position
             */
            localSubscriptions.put(columnIndexOfTheSubscriptionInBitMap, new ConcurrentHashMap<String, LocalSubscription>());

            /**
             * Divide the routing key into constituent parts
             */
            String[] destinations = destination.split("\\.");

            //Loop through the constituent parts
            for (int constituentPart = 0; constituentPart < destinations.length; constituentPart++) {
                /**
                 * When the constituent table is not created before
                 * create new one Ex - If the topic is WSO2.Products.MB
                 * But before we only had the subscriptions and topics with maximum of
                 * 2 constituentsAdd a new table for the constituent part 3
                 */
                if (bitMapLocal.size() <= constituentPart) {
                    Map<String, BitSet> newBitMapForIthConstituent;
                    newBitMapForIthConstituent = Collections.synchronizedMap(new HashMap<String, BitSet>());

                    BitSet bitSet;
                    bitSet = new BitSet(localSubscriptionCount);
                    bitSet.set(0, localSubscriptionCount);

                    for (int deleted : deletedLocals) bitSet.clear(deleted);
                    bitSet.clear(columnIndexOfTheSubscriptionInBitMap);
                    newBitMapForIthConstituent.put(SPECIAL_CHARACTER_FOR_NULL, bitSet);

                    BitSet bitSetForPreviousOther;
                    bitSetForPreviousOther = new BitSet();

                    if (constituentPart != 0) {
                        bitSetForPreviousOther = (BitSet) bitMapLocal
                                .get(constituentPart - 1).get(CONSTITUENT_TOPIC_CONSTANT).clone();
                        BitSet bitArrayForPreviousNull = (BitSet) bitMapLocal
                                .get(constituentPart - 1).get(SPECIAL_CHARACTER_FOR_NULL).clone();

                        bitSetForPreviousOther.and(bitArrayForPreviousNull);
                    }

                    BitSet bitSetForOther = bitSetForPreviousOther;
                    newBitMapForIthConstituent.put(CONSTITUENT_TOPIC_CONSTANT, bitSetForOther);
                    bitMapLocal.add(newBitMapForIthConstituent);
                }

                /** Set the bit for the subscription constituent part
                 *
                 */
                Map<String, BitSet> bitMapForithConstituent = bitMapLocal.get(constituentPart);

                if ("#".equals(destinations[constituentPart]) || "*".equals(destinations[constituentPart])) {
                    for (Map.Entry<String, BitSet> mapEntryForithBitMap : bitMapLocal.get(constituentPart).entrySet()) {
                        if (!SPECIAL_CHARACTER_FOR_NULL.equals(mapEntryForithBitMap.getKey())) {
                            mapEntryForithBitMap.getValue().set(columnIndexOfTheSubscriptionInBitMap);
                        } else {
                            if ("#".equals(destinations[constituentPart]))
                                mapEntryForithBitMap.getValue().set(columnIndexOfTheSubscriptionInBitMap);
                        }
                    }
                } else {
                    BitSet bitSetForspecificConstituentPartOfSubscription;
                    bitSetForspecificConstituentPartOfSubscription = bitMapForithConstituent.get(destinations[constituentPart]);
                    if (bitSetForspecificConstituentPartOfSubscription == null)
                        bitSetForspecificConstituentPartOfSubscription = (BitSet) bitMapLocal.get(constituentPart).get("other")
                                .clone();


                    bitSetForspecificConstituentPartOfSubscription.set(columnIndexOfTheSubscriptionInBitMap);

                    bitMapForithConstituent.put(destinations[constituentPart], bitSetForspecificConstituentPartOfSubscription);
                    bitMapLocal.remove(constituentPart);
                    bitMapLocal.add(constituentPart, bitMapForithConstituent);

                }
            }

            /**
             * If the number constituent parts in the routing key is less than the number of bitmap tables
             * set the bit for th null and also update the # accordingly
             */
            if ("#".equals(destinations[destinations.length - 1])) {
                for (int i = destinations.length; i < bitMapLocal.size(); i++) {
                    for (Map.Entry<String, BitSet> entry : bitMapLocal.get(i).entrySet())
                        entry.getValue().set(columnIndexOfTheSubscriptionInBitMap);
                }
            } else {
                for (int i = destinations.length; i < bitMapLocal.size(); i++)
                    bitMapLocal.get(i).get(SPECIAL_CHARACTER_FOR_NULL).set(columnIndexOfTheSubscriptionInBitMap);
            }

        }
        /**
         * Put the local subscription into the desired location
         */
        columnIndexOfTheSubscriptionInBitMap = localSubscriptionMapping.get(destination);

        Map<String, LocalSubscription> localSubscriptionMap = localSubscriptions.get(columnIndexOfTheSubscriptionInBitMap);
        localSubscriptionMap.put(local.getSubscriptionID(), local);
        localSubscriptions.put(columnIndexOfTheSubscriptionInBitMap, localSubscriptionMap);
    }

    /**
     * Method to add the ClusteredSubscription
     *
     * @param destination routing key of the subscription
     * @param andes       AndesSubscription
     *                    <p/>
     *                    Same logic as the localSubscription
     */

    public void addClusteredSubscription(String destination,
                                         AndesSubscription andes) {
        int columnIndexOfTheSubscriptionInBitMap;

        /**
         * If there is no any previous subscriptions with the specified routing key
         */
        if (null == clusteredSubscriptionMapping.get(destination)) {
            /**
             * If the removed subscription places still remain take the index for the subscription
             * from there
             */
            if (deletedClusters.size() > 0) {
                columnIndexOfTheSubscriptionInBitMap = deletedClusters.remove(0);
            } else {
                /**
                 * else get the index from the clusteredSubscriptionCount variable
                 */
                columnIndexOfTheSubscriptionInBitMap = clusteredSubscriptionCount;
                clusteredSubscriptionCount++;
            }

            /**
             * Put the routing key and index
             * Mapping from the routing key to index
             */
            clusteredSubscriptionMapping.put(destination, columnIndexOfTheSubscriptionInBitMap);
            /**
             * create the entry at the index position
             */
            clusteredSubscriptions.put(columnIndexOfTheSubscriptionInBitMap, new ConcurrentHashMap<String, AndesSubscription>());

            /**
             * Divide the routing key into constituent parts
             */
            String[] destinations = destination.split("\\.");

            //Loop through the constituent parts
            for (int i = 0; i < destinations.length; i++) {
                /**
                 * When the constituent table is not created before
                 * create new one Ex - If the topic is WSO2.Products.MB
                 * But before we only had the subscriptions and topics with maximum of
                 * 2 constituentsAdd a new table for the constituent part 3
                 */
                if (bitMapClustered.size() <= i) {
                    Map<String, BitSet> newBitMapForithConstituent;
                    newBitMapForithConstituent = Collections.synchronizedMap(new HashMap<String, BitSet>());

                    BitSet bitSet;
                    bitSet = new BitSet(clusteredSubscriptionCount);
                    bitSet.set(0, clusteredSubscriptionCount);

                    for (int deleted : deletedClusters) bitSet.clear(deleted);
                    bitSet.clear(columnIndexOfTheSubscriptionInBitMap);
                    newBitMapForithConstituent.put(SPECIAL_CHARACTER_FOR_NULL, bitSet);

                    BitSet bitSetForPreviousOther;
                    bitSetForPreviousOther = new BitSet();

                    if (i != 0) {
                        bitSetForPreviousOther = (BitSet) bitMapClustered
                                .get(i - 1).get(CONSTITUENT_TOPIC_CONSTANT).clone();
                        BitSet bitArrayForPreviousNull = (BitSet) bitMapClustered
                                .get(i - 1).get(SPECIAL_CHARACTER_FOR_NULL).clone();

                        bitSetForPreviousOther.and(bitArrayForPreviousNull);
                    }

                    BitSet bitSetForOther = bitSetForPreviousOther;
                    newBitMapForithConstituent.put(CONSTITUENT_TOPIC_CONSTANT, bitSetForOther);
                    bitMapClustered.add(newBitMapForithConstituent);
                }

                /** Set the bit for the subscription constituent part
                 *
                 */
                Map<String, BitSet> bitMapForIthConstituent = bitMapClustered.get(i);

                if ("#".equals(destinations[i]) || "*".equals(destinations[i])) {
                    for (Map.Entry<String, BitSet> mapEntryForIthBitMap : bitMapClustered.get(i).entrySet())
                        if (!SPECIAL_CHARACTER_FOR_NULL.equals(mapEntryForIthBitMap.getKey())) {
                            mapEntryForIthBitMap.getValue().set(columnIndexOfTheSubscriptionInBitMap);
                        } else {
                            if ("#".equals(destinations[i]))
                                mapEntryForIthBitMap.getValue().set(columnIndexOfTheSubscriptionInBitMap);
                        }
                } else {
                    BitSet bitSetForSpecificConstituentPartOfSubscription;
                    bitSetForSpecificConstituentPartOfSubscription = bitMapForIthConstituent.get(destinations[i]);
                    if ( null == bitSetForSpecificConstituentPartOfSubscription)
                        bitSetForSpecificConstituentPartOfSubscription = (BitSet) bitMapClustered.get(i).get(CONSTITUENT_TOPIC_CONSTANT)
                                .clone();


                    bitSetForSpecificConstituentPartOfSubscription.set(columnIndexOfTheSubscriptionInBitMap);

                    bitMapForIthConstituent.put(destinations[i], bitSetForSpecificConstituentPartOfSubscription);
                    bitMapClustered.remove(i);
                    bitMapClustered.add(i, bitMapForIthConstituent);

                }
            }

            /**
             * If the number constituent parts in the routing key is less than the number of bitmap tables
             * set the bit for th null and also update the # accordingly
             */
            if ("#".equals(destinations[destinations.length - 1])) {
                for (int i = destinations.length; i < bitMapClustered.size(); i++) {
                    for (Map.Entry<String, BitSet> entry : bitMapClustered.get(i).entrySet())
                        entry.getValue().set(columnIndexOfTheSubscriptionInBitMap);
                }
            } else {
                for (int i = destinations.length; i < bitMapClustered.size(); i++)
                    bitMapClustered.get(i).get(SPECIAL_CHARACTER_FOR_NULL).set(columnIndexOfTheSubscriptionInBitMap);
            }

        }
        /**
         * Put the andes subscription into the desired location
         */
        columnIndexOfTheSubscriptionInBitMap = clusteredSubscriptionMapping.get(destination);

        Map<String, AndesSubscription> clusteredSubscriptionMap;
        clusteredSubscriptionMap = clusteredSubscriptions.get(columnIndexOfTheSubscriptionInBitMap);
        clusteredSubscriptionMap.put(andes.getSubscriptionID(), andes);
        clusteredSubscriptions.put(columnIndexOfTheSubscriptionInBitMap, clusteredSubscriptionMap);
    }

    /**
     * To remove the subscription and to update the bitmaps
     *
     * @param toBeRemoved index of the subscription to be removed
     * @param isLocal     indicates local or andes subscription
     */
    private void removeSubscription(int toBeRemoved, boolean isLocal) {
        ArrayList<Map<String, BitSet>> bitMap = isLocal ? bitMapLocal
                : bitMapClustered;

        // clear the bit related with the deletion of subscription
        for (int i = 0; i < bitMap.size(); i++) {
            Map<String, BitSet> bitSetMap = bitMap.get(i);
            for (Map.Entry<String, BitSet> entryMap : bitSetMap.entrySet()) entryMap.getValue().clear(toBeRemoved);
            bitMap.remove(i);
            bitMap.add(i, bitSetMap);
        }

    }

    /**
     * Method to remove the local subscription and update the bitMaps
     *
     * @param subscriptionID subscription ID of the subscription need to be removed
     */
    public void removeLocalSubscription(String subscriptionID) {
        for (Map.Entry<Integer, Map<String, LocalSubscription>> mapEntry : localSubscriptions.entrySet()) {
            if (mapEntry.getValue().get(subscriptionID) != null) {
                String destination = localSubscriptions.get(mapEntry.getKey()).get(subscriptionID).getSubscribedDestination();
                mapEntry.getValue().remove(subscriptionID);
                //remove the entry if the subscription count with the particular size goes to zero
                if (0 == mapEntry.getValue().size()) {
                    localSubscriptions.remove(mapEntry.getKey());
                    localSubscriptionMapping.remove(destination);
                    removeSubscription(mapEntry.getKey(), true);
                    deletedLocals.add(mapEntry.getKey());
                }
                return;
            }
        }
    }

    /**
     * Method to remove the clustered subscription and update the bitmaps
     *
     * @param subscriptionID subscription ID of the subscription need to be removed
     */
    public void removeClusteredSubscription(String subscriptionID) {
        for (Map.Entry<Integer, Map<String, AndesSubscription>> mapEntry : clusteredSubscriptions.entrySet()) {
            if (mapEntry.getValue().get(subscriptionID) != null) {
                String destination = clusteredSubscriptions.get(mapEntry.getKey()).get(subscriptionID).getSubscribedDestination();
                mapEntry.getValue().remove(subscriptionID);
                //remove the entry if the subscription count with the particular size goes to zero
                if (0 == mapEntry.getValue().size()) {
                    clusteredSubscriptions.remove(mapEntry.getKey());
                    clusteredSubscriptionMapping.remove(destination);
                    removeSubscription(mapEntry.getKey(), false);
                    deletedClusters.add(mapEntry.getKey());
                }
                return;
            }
        }

    }

    /**
     * To get the matching subscriptions for a destination
     *
     * @param destination routing pattern of the message
     */
    public ArrayList<LocalSubscription> findMatchingLocalSubscriptions(String destination) {
        /**
         * Split the routing key of the subscriptions into the constituent parts
         */
        String[] destinations = destination.split("\\.");
        java.util.BitSet results = new java.util.BitSet();
        ArrayList<LocalSubscription> matchingSubscriptionsForTheMessage;
        matchingSubscriptionsForTheMessage = new ArrayList<LocalSubscription>();

        /**
         * Loop until the minimum constituent parts of the bitmap size and the routing size
         *
         */
        for (int constituentCount = 0; constituentCount < bitMapLocal.size() && constituentCount < destinations.length; constituentCount++) {

            Map<String, BitSet> bitMapOfIthConstituent = bitMapLocal.get(constituentCount);
            BitSet bitSetOfIthConstituent;


            // Take the entry for the constituent part
            if (null != bitMapOfIthConstituent.get(destinations[constituentCount]))
                bitSetOfIthConstituent = (BitSet) bitMapOfIthConstituent.get(destinations[constituentCount]).clone();
            else
                bitSetOfIthConstituent = (BitSet) bitMapOfIthConstituent.get(CONSTITUENT_TOPIC_CONSTANT).clone();

            if (constituentCount != 0) {
                results.and(bitSetOfIthConstituent);
            } else
                results = bitSetOfIthConstituent;

        }

        if (destinations.length != bitMapLocal.size()) {
            /**
             * If the number of constituent parts is greater than the number of bitmaps for the constituent
             * part get the and with the # entry
             *
             */

            if (bitMapLocal.size() != 0 && destinations.length > bitMapLocal.size()) {
                Map<String, BitSet> map = bitMapLocal
                        .get(bitMapLocal.size() - 1);
                BitSet bitSetForOther = (BitSet) map.get(CONSTITUENT_TOPIC_CONSTANT).clone();
                BitSet bitSetForNull = (BitSet) map.get(SPECIAL_CHARACTER_FOR_NULL).clone();
                bitSetForOther.and(bitSetForNull);
                results.and(bitSetForOther);
            }

            /**
             * If the number of constituent parts in the bitmap is greater and with the null and # entry bitmap
             */
            else if (destinations.length != 0 && bitMapLocal.size() > destinations.length) {
                Map<String, BitSet> map = bitMapLocal
                        .get(destinations.length);
                BitSet bitSetForNull = map.get(SPECIAL_CHARACTER_FOR_NULL);
                results.and(bitSetForNull);

            }

        }

        for (int numberOfLocalSubscriptions = 0; numberOfLocalSubscriptions < localSubscriptionCount; ) {
            int nextMatchingLocalSubIndex = results.nextSetBit(numberOfLocalSubscriptions);
            if (nextMatchingLocalSubIndex >= 0) {
                for (LocalSubscription l : localSubscriptions.get(nextMatchingLocalSubIndex).values()) {
                    if (l.hasExternalSubscriptions())
                        matchingSubscriptionsForTheMessage.add(l);
                }

                numberOfLocalSubscriptions = nextMatchingLocalSubIndex + 1;
            } else {
                numberOfLocalSubscriptions = localSubscriptionCount;
            }
        }

        return matchingSubscriptionsForTheMessage;
    }

    /**
     * To get the matching clustered subscriptions for a destination
     *
     * @param destination routing pattern of the message
     *                    Same logic as the local subscriptions
     */
    public ArrayList<AndesSubscription> findMatchingClusteredSubscriptions(String destination) {
        String[] destinations = destination.split("\\.");
        java.util.BitSet results = new java.util.BitSet();
        ArrayList<AndesSubscription> matchingSubscriptionsForTheMessage = new ArrayList<AndesSubscription>();

        for (int i = 0; i < bitMapClustered.size() && i < destinations.length; i++) {
            Map<String, BitSet> bitMapForIthConstituent = bitMapClustered.get(i);

            BitSet bitSetForIthConstituent;


            if (null != bitMapForIthConstituent.get(destinations[i]))
                bitSetForIthConstituent = (java.util.BitSet) bitMapForIthConstituent.get(destinations[i]).clone();
            else
                bitSetForIthConstituent = (java.util.BitSet) bitMapForIthConstituent.get(CONSTITUENT_TOPIC_CONSTANT).clone();

            if (i != 0)
                results.and(bitSetForIthConstituent);
            else
                results = bitSetForIthConstituent;

        }

        if (!(destinations.length == bitMapClustered.size())) {

            if (bitMapClustered.size() != 0 && destinations.length > bitMapClustered.size()) {
                Map<String, BitSet> map = bitMapClustered.get(bitMapClustered
                        .size() - 1);
                BitSet bitSetForOther = (BitSet) map.get(CONSTITUENT_TOPIC_CONSTANT).clone();
                BitSet bitSetForNull = (BitSet) map.get(SPECIAL_CHARACTER_FOR_NULL).clone();

                bitSetForOther.and(bitSetForNull);
                results.and(bitSetForOther);

            } else if (destinations.length != 0 && bitMapClustered.size() > destinations.length) {
                Map<String, BitSet> map = bitMapClustered
                        .get(destinations.length);
                BitSet bitSetForOther = map.get(SPECIAL_CHARACTER_FOR_NULL);

                results.and(bitSetForOther);

            }
        }

        for (int i = 0; i < clusteredSubscriptionCount; ) {
            int k = results.nextSetBit(i);
            if (k >= 0) {
                matchingSubscriptionsForTheMessage.addAll(clusteredSubscriptions.get(k).values());
                i = k + 1;
            } else {
                i = clusteredSubscriptionCount;
            }
        }
        return matchingSubscriptionsForTheMessage;
    }

    /**
     * To get all the andes subscriptions which are subscribed for a destination
     *
     * @param destination destination required
     * @return the andes subscriptions which are subscribed for a destination
     */
    public List<AndesSubscription> getAllClusteredSubscribedForDestination(String destination) {
        if (null != clusteredSubscriptionMapping.get(destination)) {
            int number = clusteredSubscriptionMapping.get(destination);
            return new ArrayList<AndesSubscription>(clusteredSubscriptions.get(number).values());
        }
        return null;

    }

    /**
     * To get all the local subscriptions that are subscribed for a destination
     * @param destination specific destination
     * @return the local subscriptions which are subscribed for a destination
     */
    public Map<String, LocalSubscription> getAllLocalSubscribedForDestination(String destination) {
        if (null != localSubscriptionMapping.get(destination)) {
            int mappingOfDestination = localSubscriptionMapping.get(destination);

            return localSubscriptions.get(mappingOfDestination);
        }
        return null;

    }

    /**
     * To get all the destinations which have the clustered subscriptions
     * @return the all destinations which have the clustered subscription
     */
    public List<String> getAllDestinationsOfSubscriptions() {
        return new ArrayList<String>(clusteredSubscriptionMapping.keySet());

    }

    /**
     * To get the Andes Subscriptions and the subscription ID
     * @return the Andes Subscriptions and the subscription ID
     */
    public Collection<Map<String, AndesSubscription>> getClusteredSubscriptions() {
        return clusteredSubscriptions.values();
    }

    /**
     * To get the Local Subscriptions and the subscription ID
     * @return the Local Subscriptions and the subscription ID
     */
    public Collection<Map<String, LocalSubscription>> getLocalSubscriptions() {
        return localSubscriptions.values();
    }

    /**
     * Method to replace the all the previous subscriptions for a destination with a new list
     * @param destination destination to be replaced
     * @param newSubscriptionList new list of subscriptions
     * @return the old list of the subscriptions
     */
    public List<AndesSubscription> getAllClustered(String destination, List<AndesSubscription> newSubscriptionList) {
        List<AndesSubscription> oldList = null;
        if (null != clusteredSubscriptionMapping.get(destination)) {
            int mappingIndex = clusteredSubscriptionMapping.get(destination);
            oldList = new ArrayList<AndesSubscription>(clusteredSubscriptions.remove(mappingIndex).values());
            Map<String, AndesSubscription> newList = new ConcurrentHashMap<String, AndesSubscription>();
            for (AndesSubscription andesSubscription : newSubscriptionList) {
                newList.put(andesSubscription.getSubscriptionID(), andesSubscription);
            }
            clusteredSubscriptions.put(mappingIndex, newList);
        } else {
            for (AndesSubscription andesSubscription : newSubscriptionList) {
                addClusteredSubscription(destination, andesSubscription);
            }
        }
        return oldList;
    }
}
