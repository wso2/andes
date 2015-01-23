package org.wso2.andes.subscription;

import org.wso2.andes.kernel.AndesSubscription;
import org.wso2.andes.kernel.LocalSubscription;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
/*
 * Copyright (c) 2005-2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

/**
 * This class is responsible for adding of the subscriptions into the bitmap, matching and the removal
 * of subscriptions from bitmap
 */
public class SubscriptionBitMap {
    /**
     * Keeps track of the local subscriptions
     *
     * Map <Index of the subscriber, Map<Subscription ID, LocalSubscription>
     */
    private Map<Integer, Map<String, LocalSubscription>> localSubscriptions;

    /**
     * Keeps track of the Andes subscriptions
     *
     * Map <Index of the subscriber, Map<Subscription ID, AndesSubscription
     * Ex Suppose if we have three subscribers WSO2, WSO2.#, WSO2.MB
     *    Then the subscribers will get the indice 0, 1, 2
     */

    private Map<Integer, Map<String, AndesSubscription>> clusteredSubscriptions;

    /**
     * Mapping from the routing key to the integer
     *
     * Map<routing key, index>
     * Example : Subscriptions WSO2, WSO2.*, WSO2.MB
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
     * Represents the mapping from criteria to local subscriptions
     *
     * ArrayList of Bitmap tables
     *
     * Each bit map table Map<Constituent part, BitMap for the constituent part >
     */
    private ArrayList<Map<String, SubscriptionBitArray>> bitMapLocal;

    /**
     * Represents the mapping from criteria to clustered subscriptions
     */
    protected ArrayList<Map<String, SubscriptionBitArray>> bitMapClustered;

    /**
     * Contains the details about the local subscriptions which contains the #
     * character and the location
     */
    private Map<Integer, Integer> hasSpecialCharactersLocal;

    /**
     * Contains the details about the clustered subscriptions which contains the
     * # character and the location
     */
    private Map<Integer, Integer> hasSpecialCharactersClustered;
    /**
     * Keeps track of the deleted local subscription indexes
     * Whenever this arraylist is not empty, the index for the new subscription is found from this
     *
     */
    private ArrayList<Integer> deletedLocals;

    /**
     * Keeps track of the deleted clustered subscription indexes
     */
    private ArrayList<Integer> deletedClusters;

    /**
     * Initialize the BitMap
     */
    public SubscriptionBitMap() {
        localSubscriptions = new ConcurrentHashMap<Integer, Map<String, LocalSubscription>>();
        clusteredSubscriptions = new ConcurrentHashMap<Integer, Map<String, AndesSubscription>>();
        localSubscriptionMapping = new ConcurrentHashMap<String, Integer>();
        clusteredSubscriptionMapping = new ConcurrentHashMap<String, Integer>();
        bitMapLocal = new ArrayList<Map<String, SubscriptionBitArray>>();
        bitMapClustered = new ArrayList<Map<String, SubscriptionBitArray>>();
        hasSpecialCharactersLocal = new ConcurrentHashMap<Integer, Integer>();
        hasSpecialCharactersClustered = new ConcurrentHashMap<Integer, Integer>();
        localSubscriptionCount = 0;
        clusteredSubscriptionCount = 0;
        deletedClusters = new ArrayList<Integer>();
        deletedLocals = new ArrayList<Integer>();
    }

    /**
     * Method to add the LocalSubscription
     *
     * @param destination routing key of the LocalSubscription
     * @param local LocalSubscription to be added
     */
    public void addLocalSubscription(String destination, LocalSubscription local) {
        int columnIndexOfTheSubscriptionInBitMap;

        /**
         * If there is no any previous subscriptions with the specified routing key
         */
        if(null == localSubscriptionMapping.get(destination)) {
            /**
             * If the removed subscription places still remain take the index for the subscription
             * from there
             */
            if(deletedLocals.size() > 0) {
                columnIndexOfTheSubscriptionInBitMap = deletedLocals.remove(0);
            }
            else {
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
            localSubscriptions.put(columnIndexOfTheSubscriptionInBitMap,new HashMap<String, LocalSubscription>());

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
                if (bitMapLocal.size() <= i) {
                    Map<String, SubscriptionBitArray> map = Collections.synchronizedMap(new HashMap<String, SubscriptionBitArray>());
                    map.put("other", new SubscriptionBitArray(localSubscriptionCount));
                    map.put("*", new SubscriptionBitArray(localSubscriptionCount));
                    SubscriptionBitArray subscriptionBitArray = new SubscriptionBitArray(localSubscriptionCount);
                    BitSet bit = subscriptionBitArray.getBits();
                    bit.set(0, localSubscriptionCount);
                    subscriptionBitArray.changeBitArray(bit);

                    for (int deleted : deletedLocals) subscriptionBitArray.clearBit(deleted);
                    subscriptionBitArray.clearBit(columnIndexOfTheSubscriptionInBitMap);
                    map.put(null, subscriptionBitArray);
                    SubscriptionBitArray b = new SubscriptionBitArray(localSubscriptionCount);
                    for (Integer j : hasSpecialCharactersLocal.keySet()) {
                        b.setBit(j);
                    }

                    map.put("#", b);
                    bitMapLocal.add(map);
                }

                /** Set the bit for the subscription constituent part
                 *
                 */
                Map<String, SubscriptionBitArray> temp = bitMapLocal.get(i);
                SubscriptionBitArray array = temp.get(destinations[i]);
                if (array == null)
                    array = new SubscriptionBitArray(localSubscriptionCount);

                array.setBit(columnIndexOfTheSubscriptionInBitMap);

                temp.put(destinations[i], array);
                bitMapLocal.remove(i);
                bitMapLocal.add(i, temp);

            }

            /**
             * If the number constituent parts in the routing key is less than the number of bitmap tables
             * set the bit for th null and also update the # accordingly
             */
            for (int i = destinations.length; i < bitMapLocal.size(); i++) {
                Map<String, SubscriptionBitArray> map = bitMapLocal.get(i);
                if (destinations[destinations.length - 1].equals("#"))
                    map.get("#").setBit(columnIndexOfTheSubscriptionInBitMap);
                map.get(null).setBit(columnIndexOfTheSubscriptionInBitMap);
            }
            /**
             * Add the index to the arraylist which keeps track of the subscriptions which has "#" value
             */
            if (destinations[destinations.length - 1].equals("#")) {
                hasSpecialCharactersLocal.put(columnIndexOfTheSubscriptionInBitMap, columnIndexOfTheSubscriptionInBitMap);
            }
        }

        /**
         * Put the local subscription into the arraylist
         */
        columnIndexOfTheSubscriptionInBitMap  = localSubscriptionMapping.get(destination);

        Map<String, LocalSubscription> temp= localSubscriptions.get(columnIndexOfTheSubscriptionInBitMap);
        temp.put(local.getSubscriptionID(), local);
        localSubscriptions.put(columnIndexOfTheSubscriptionInBitMap, temp);
    }

    /**
     * Method to add the ClusteredSubscription
     *
     * @param destination routing key of the subscription
     * @param andes AndesSubscription
     *
     * Same logic as the localSubscription
     */
    public void addClusteredSubscription(String destination,
                                         AndesSubscription andes) {
        if(clusteredSubscriptionMapping.get(destination) == null) {
            int columnIndexOfTheSubscriptionInBitMap;
            if(deletedClusters.size() > 0) {
                columnIndexOfTheSubscriptionInBitMap = deletedClusters.remove(0);
            }
            else {
                columnIndexOfTheSubscriptionInBitMap = clusteredSubscriptionCount;
                clusteredSubscriptionCount++;
            }
            clusteredSubscriptionMapping.put(destination, columnIndexOfTheSubscriptionInBitMap);
            clusteredSubscriptions.put(columnIndexOfTheSubscriptionInBitMap,new HashMap<String, AndesSubscription>());


            String[] destinations = destination.split("\\.");


            for (int i = 0; i < destinations.length; i++) {
                if (bitMapClustered.size() <= i) {
                    Map<String, SubscriptionBitArray> map = Collections.synchronizedMap(new HashMap<String, SubscriptionBitArray>());
                    map.put("other", new SubscriptionBitArray(clusteredSubscriptionCount));
                    map.put("*", new SubscriptionBitArray(clusteredSubscriptionCount));
                    SubscriptionBitArray b1 = new SubscriptionBitArray(clusteredSubscriptionCount);
                    BitSet bit = b1.getBits();
                    bit.set(0, clusteredSubscriptionCount);
                    b1.changeBitArray(bit);

                    for (int deleted : deletedClusters) {
                        b1.clearBit(deleted);
                    }
                    b1.clearBit(columnIndexOfTheSubscriptionInBitMap);
                    map.put(null, b1);

                    SubscriptionBitArray b = new SubscriptionBitArray(clusteredSubscriptionCount);
                    for (Integer j : hasSpecialCharactersClustered.keySet()) {
                        b.setBit(j);
                    }
                    map.put("#", b);
                    bitMapClustered.add(map);
                }

                Map<String, SubscriptionBitArray> temp = bitMapClustered.get(i);
                SubscriptionBitArray array = temp.get(destinations[i]);
                if (array == null)
                    array = new SubscriptionBitArray(clusteredSubscriptionCount);

                array.setBit(columnIndexOfTheSubscriptionInBitMap);
                temp.put(destinations[i], array);
                bitMapClustered.remove(i);
                bitMapClustered.add(i, temp);
            }
            for (int i = destinations.length; i < bitMapClustered.size(); i++) {
                Map<String, SubscriptionBitArray> map = bitMapClustered.get(i);
                if (destinations[destinations.length - 1].equals("#"))
                    map.get("#").setBit(columnIndexOfTheSubscriptionInBitMap);
                map.get(null).setBit(columnIndexOfTheSubscriptionInBitMap);
            }
            if (destinations[destinations.length - 1].equals("#")) {
                hasSpecialCharactersClustered.put(columnIndexOfTheSubscriptionInBitMap, columnIndexOfTheSubscriptionInBitMap);
            }
        }
        int columnIndexOfTheSubscriptionInBitMap  = clusteredSubscriptionMapping.get(destination);

        Map<String, AndesSubscription> temp= clusteredSubscriptions.get(columnIndexOfTheSubscriptionInBitMap);
        temp.put(andes.getSubscriptionID(), andes);
        clusteredSubscriptions.put(columnIndexOfTheSubscriptionInBitMap, temp);

    }

    /**
     * To remove the subscription and to update the bitmaps
     *
     * @param toberemoved index of the subscription to be removed
     * @param isLocal indicates local or andes subscription
     */
    private void removeSubscription(int toberemoved, boolean isLocal) {
        ArrayList<Map<String, SubscriptionBitArray>> bitMap = isLocal ? bitMapLocal
                : bitMapClustered;

        // clear the bit related with the deletion of subscription
        for (int i = 0; i < bitMap.size(); i++) {
            Map<String, SubscriptionBitArray> temp = bitMap.get(i);
            for (String s : temp.keySet()) {
                temp.get(s).clearBit(toberemoved);
            }
            bitMap.remove(i);
            bitMap.add(i, temp);
        }

        /**
         * Remove the entry form the hash maps
         */
        if (isLocal) {
            hasSpecialCharactersLocal.remove(toberemoved);
            bitMapLocal = bitMap;
        } else {
            hasSpecialCharactersClustered.remove(toberemoved);
            bitMapClustered = bitMap;
        }

    }

    /**
     * Method to remove the local subscription and update the bitMaps
     *
     * @param subscriptionID
     *            subscription ID of the subscription need to be removed
     */
    public void removeLocalSubscription(String subscriptionID) {
        for (Integer i : localSubscriptions.keySet()) {
            if (localSubscriptions.get(i).get(subscriptionID) != null) {
                String destination = localSubscriptions.get(i).get(subscriptionID).getSubscribedDestination();
                localSubscriptions.get(i).remove(subscriptionID);
                //remove the entry if the subscription count with the particular size goes to zero
                if(localSubscriptions.get(i).size() == 0) {
                    localSubscriptions.remove(i);
                    localSubscriptionMapping.remove(destination);
                    removeSubscription(i, true);
                    deletedLocals.add(i);
                }
                return;
            }

        }
    }

    /**
     * Method to remove the clustered subscription and update the bitmaps
     *
     * @param subscriptionID
     *            subscription ID of the subscription need to be removed
     */
    public void removeClusteredSubscription(String subscriptionID) {
        for (Integer i : clusteredSubscriptions.keySet()) {
            if (clusteredSubscriptions.get(i).get(subscriptionID) != null) {
                String destination = clusteredSubscriptions.get(i).get(subscriptionID).getSubscribedDestination();
                clusteredSubscriptions.get(i).remove(subscriptionID);
                if(clusteredSubscriptions.get(i).size() == 0) {
                    clusteredSubscriptions.remove(i);
                    clusteredSubscriptionMapping.remove(destination);
                    removeSubscription(i, false);
                    deletedClusters.add(i);
                }
                return;
            }
        }

    }

    /**
     * To get the matching subscriptions for a destination
     * @param destination routing pattern of the message
     */
    public ArrayList<LocalSubscription> getMatching(String destination) {
        /**
         * Split the routing key of the subscriptions into the constituent parts
         */
        String[] destinations = destination.split("\\.");
        BitSet results = new BitSet();
        ArrayList<LocalSubscription> temp = new ArrayList<LocalSubscription>();

        /**
         * Loop until the minimum constituent parts of the bitmap size and the routing size
         *
         */
        for (int i = 0; i < bitMapLocal.size() && i < destinations.length; i++) {

            Map<String, SubscriptionBitArray> map = bitMapLocal.get(i);
            SubscriptionBitArray b = map.get(destinations[i]);
            SubscriptionBitArray b1 = map.get("other");
            BitSet array1;
            BitSet array2;

            // Take the entry for the constituent part
            if (null != map.get(destinations[i]))
                array1 = (BitSet) b.getBits().clone();
            else
                array1 = (BitSet) b1.getBits().clone();

            // or with the bitmap of the * entry as * represents one word
            array2 = map.get("*").getBits();
            array1.or(array2);
            // or with the bit map of the entry as # represents zero or more words
            array1.or(map.get("#").getBits());

            if (i != 0)
                results.and(array1);
            else
                results = array1;

        }

        /**
         * If the number of constituent parts is greater than the number of bitmaps for the constituent
         * part get the and with the # entry
         *
         */
        if (destinations.length > bitMapLocal.size()) {
            Map<String, SubscriptionBitArray> map = bitMapLocal
                    .get(bitMapLocal.size() - 1);
            results.and(map.get("#").getBits());
        }

        /**
         * If the number of constituent parts in the bitmap is greater and with the null and # entry bitmap
         */
        if (bitMapLocal.size() > destinations.length) {
            Map<String, SubscriptionBitArray> map = bitMapLocal
                    .get(destinations.length);
            BitSet t = map.get(null).getBits();
            t.or(map.get("#").getBits());
            results.and(t);

        }


        for (int i = 0; i < localSubscriptionCount;) {
            int k = results.nextSetBit(i);
            if (k >= 0) {
                for (LocalSubscription l : localSubscriptions.get(k).values()) {
                    if (l.hasExternalSubscriptions())
                        temp.add(l);
                }

                i = k + 1;
            } else {
                i = localSubscriptionCount;
            }
        }

        return temp;
    }

    /**
     * To get the matching clustered subscriptions for a destination
     *
     * @param destination routing pattern of the message
     * Same logic as the local subscriptions
     */
    public ArrayList<AndesSubscription> getMatchingAndes(String destination) {
        String[] destinations = destination.split("\\.");
        BitSet results = new BitSet();
        ArrayList<AndesSubscription> temp = new ArrayList<AndesSubscription>();

        for (int i = 0; i < bitMapClustered.size() && i < destinations.length; i++) {
            Map<String, SubscriptionBitArray> map = bitMapClustered.get(i);

            SubscriptionBitArray subscriptionBitArray = map.get(destinations[i]);
            SubscriptionBitArray b1 = map.get("other");
            BitSet bitSetOfSubscriptionBitArray;
            BitSet bitSetOfSubscriptionBitArrayOfOther;

            if (null != map.get(destinations[i]))
                bitSetOfSubscriptionBitArray = (BitSet) subscriptionBitArray.getBits().clone();
            else
                bitSetOfSubscriptionBitArray = (BitSet) b1.getBits().clone();

            bitSetOfSubscriptionBitArrayOfOther = map.get("*").getBits();
            bitSetOfSubscriptionBitArray.or(bitSetOfSubscriptionBitArrayOfOther);
            bitSetOfSubscriptionBitArray.or(map.get("#").getBits());

            if (i != 0)
                results.and(bitSetOfSubscriptionBitArray);
            else
                results = bitSetOfSubscriptionBitArray;

        }

        if (destinations.length > bitMapClustered.size()) {
            Map<String, SubscriptionBitArray> map = bitMapClustered.get(bitMapClustered
                    .size() - 1);
            results.and(map.get("#").getBits());

        }
        if (bitMapClustered.size() > destinations.length) {
            Map<String, SubscriptionBitArray> map = bitMapClustered
                    .get(destinations.length);
            BitSet t = map.get(null).getBits();
            t.or(map.get("#").getBits());
            results.and(t);

        }

        for (int i = 0; i < clusteredSubscriptionCount;) {
            int k = results.nextSetBit(i);
            if (k >= 0) {
                temp.addAll(clusteredSubscriptions.get(k).values());
                i = k + 1;
            } else {
                i = clusteredSubscriptionCount;
            }
        }
        return temp;
    }

    public List<AndesSubscription> getAllSubscribedForDestination(String destination) {
        if(null != clusteredSubscriptionMapping.get(destination)){
            int number = clusteredSubscriptionMapping.get(destination);
            return new ArrayList<AndesSubscription>(clusteredSubscriptions.get(number).values());
        }
        return null;

    }

    public Map<String,LocalSubscription> getAllLocalSubscribedForDestination(String destination) {
        if(null != localSubscriptionMapping.get(destination)){
            int number = localSubscriptionMapping.get(destination);

            return localSubscriptions.get(number);
        }
        return null;

    }


    public List<String> getAllDestinationsOfSubscriptions() {
        return new ArrayList<String>(clusteredSubscriptionMapping.keySet());

    }

    public Collection<Map<String, AndesSubscription>> getClusteredSubscriptions() {
        return clusteredSubscriptions.values();
    }

    public Collection<Map<String, LocalSubscription>> getLocalSubscriptions() {
        return localSubscriptions.values();
    }

    public List<AndesSubscription> getAllClustered(String destination, List<AndesSubscription> newSubscriptionList){
        List<AndesSubscription> oldList = null;
        if(null != clusteredSubscriptionMapping.get(destination)){
            int mappingIndex = clusteredSubscriptionMapping.get(destination);
            oldList= new ArrayList<AndesSubscription>(clusteredSubscriptions.remove(mappingIndex).values());
            Map<String, AndesSubscription> newList = new ConcurrentHashMap<String, AndesSubscription>();
            for(AndesSubscription andesSubscription : newSubscriptionList) {
                newList.put(andesSubscription.getSubscriptionID(), andesSubscription);
            }
            clusteredSubscriptions.put(mappingIndex, newList);
        }else {
            for(AndesSubscription andesSubscription : newSubscriptionList) {
                addClusteredSubscription(destination, andesSubscription);
            }
        }
        return oldList;
    }
}
