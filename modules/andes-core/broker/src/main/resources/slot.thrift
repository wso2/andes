namespace java org.wso2.andes.server.slotImp.thrift.gen

typedef i64 long // Define long type

/* A Slot consists of followings
 * messageCount - number of messages in the slotImp
 * startMessageId - starting message ID of the slotImp
 * endMessageId - ending message ID of the slotImp
 * queue - the queue which the slotImp belongs to
 */
struct SlotInfo {
    1: optional long  messageCount;
    2: long  startMessageId;
    3: long  endMessageId;
    4: string queue;
}

/*
    the services provided to update and get information of slots in slotImp manager
*/
service SlotManagementService {
    /* The getSlot operation. This method is used to get a slotImp from SlotManager
    */
    SlotInfo getSlot(1: string queueName),
    /* The updateMessageId operation is to update the message ID in the coordinator after chunk of messages are published
    */
    void updateMessageId(1: string queueName, 2: long messageId)

}