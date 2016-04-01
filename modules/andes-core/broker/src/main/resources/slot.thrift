namespace java org.wso2.andes.thrift.slot.gen

//typedef i64 long

/* A Slot consists of followings
 * messageCount - number of messages in the slotImp
 * startMessageId - starting message ID of the slotImp
 * endMessageId - ending message ID of the slotImp
 * queueName - the queueName which the slotImp belongs to
 */
struct SlotInfo {
    1: optional i64  messageCount;
    2: string slotRangesString;
    4: i64  queueId;
    5: i64 assignedNodeId;
    6: bool hasOverlappingSlots;
    
}

/*
    the services provided to update and get information of slots in slotImp manager
*/
service SlotManagementService {
    /* The getSlot operation. This method is used to get a slotImp from SlotManager
    */
    SlotInfo getSlotInfo(1: i64 storageQueueId, 2: i64 nodeId),

    /* The updateMessageId operation is to update the message ID in the coordinator after chunk of messages are published.
    *  In addition, the coordinator will check if the received slot overlaps with any existing,assigned slots, 
    *  and memorize such ranges to be given back to the same node.
    */
    void updateMessageId(1: string queueName, 2: string nodeId, 3: i64 startMessageId, 4: i64
     endMessageId, 5: i64 localSafeZone),

    /* Whenever the written message count amounts to a total divisible by SLOT_WINDOW_SIZE, or if the publisher is idle after publishing a 
    *  lesser message count, this method is called from the node to define a slot range with the coordinator. 
    *  The parameter is a bytebuffer containing 3 long values. 1) nodeID, 2) storageQueueID 3) messageID.
    */
    void communicateQueueWiseSlot(1: binary messageIdentifiers),

    /* Delete empty slots
    */
    bool deleteSlot(1: string queueName, 2: SlotInfo slotInfo, 3: string nodeId),

    /* Re-assign the slot when there are no local subscribers in the node
    */
    void reAssignSlotWhenNoSubscribers(1: string nodeId, 2: string queueName),

    /* This is used by nodes to communicate their current generated message ID, so that the coordinator can decide the minimal message ID 
    *  to derive the safe zone for deleting slots.
    */
    i64 updateCurrentMessageIdForSafeZone(1: i64 messageId, 2: string nodeId),

    /**
     * Delete all in-memory slot associations with a given queue. This is required to handle a queue purge event.
     *
     * @param queueName name of destination queue
     */
    void clearAllActiveSlotRelationsToQueue(1: string queueName)

}