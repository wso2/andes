namespace java org.wso2.andes.server.slot.thrift.gen

//typedef i64 long

/* A Slot consists of followings
 * messageCount - number of messages in the slotImp
 * startMessageId - starting message ID of the slotImp
 * endMessageId - ending message ID of the slotImp
 * queueName - the queueName which the slotImp belongs to
 */
struct SlotInfo {
    1: optional i64  messageCount;
    2: i64  startMessageId;
    3: i64  endMessageId;
    4: string queueName;
}

/*
    the services provided to update and get information of slots in slotImp manager
*/
service SlotManagementService {
    /* The getSlot operation. This method is used to get a slotImp from SlotManager
    */
    SlotInfo getSlotInfo(1: string queueName, 2: string nodeId),

    /* The updateMessageId operation is to update the message ID in the coordinator after chunk of messages are published
    */
    void updateMessageId(1: string queueName, 2: i64 messageId),

    /*delete empty slots
    */
    void deleteSlot(1: string queueName, 2: SlotInfo slotInfo, 3: string nodeId),

    /*re-assign the slot when there are no local subscribers in the node
    */
    void reAssignSlotWhenNoSubscribers(1: string nodeId, 2: string queueName)

}