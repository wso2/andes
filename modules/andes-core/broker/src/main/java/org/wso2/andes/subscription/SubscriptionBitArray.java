package org.wso2.andes.subscription;

import java.util.BitSet;

/*
 * Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
 * Represents the bit array which contain details of the mapping of criteria to
 * subscription
 *
 * @author WSO2
 *
 */
public class SubscriptionBitArray {
    /**
     * Bit set representing a specific criteria Bit is set to 1 if a
     * subscription is subscribed to a specific topic
     */
    private BitSet bitList;

    /**
     * Constructs the Bit Array with no elements
     */
    public SubscriptionBitArray() {
        bitList = new BitSet();
    }

    /**
     * Initialize the BitSet with the pre-specified length
     *
     * @param nbits
     */
    public SubscriptionBitArray(int nbits) {
        bitList = new BitSet(nbits);
    }

    /**
     * Initializes the BitArray with the pre-specified BitSet
     *
     * @param bitList
     *            pre-specified BitSet
     */
    public SubscriptionBitArray(BitSet bitList) {
        this.bitList = bitList;
    }

    /**
     * Method to set a particular location of the bit array to 1
     *
     * @param bitIndex
     *            index for which the bit should be set to 1;
     */
    public void setBit(int bitIndex) {
        bitList.set(bitIndex);
    }

    /**
     * Method to clear a bit in the particular position
     *
     * @param bitIndex
     *            index for which the bit should be set to 0
     */
    public void clearBit(int bitIndex) {
        bitList.clear(bitIndex);
    }

    /**
     * To change the bitarray
     *
     * @param bitList
     */
    public void changeBitArray(BitSet bitList) {
        this.bitList = bitList;
    }

    
    /**
     * To get the bitList
     *
     */
    public BitSet getBits() {
        return bitList;
    }
}
