package org.wso2.andes.server.cassandra;

/**
 * Created with IntelliJ IDEA.
 * User: sajini
 * Date: 7/29/14
 * Time: 4:44 PM
 * To change this template use File | Settings | File Templates.
 */
public class Slot {

    private long slotNo;
    private long messageCount;
    private String startMessageId;
    private String endMessageId;


    public String getStartMessageId() {
        return startMessageId;
    }

    public void setStartMessageId(String startMessageId) {
        this.startMessageId = startMessageId;
    }

    public long getSlotNo() {
        return slotNo;
    }

    public void setSlotNo(long slotNo) {
        this.slotNo = slotNo;
    }

    public long getMessageCount() {
        return messageCount;
    }

    public void setMessageCount(long messageCount) {
        this.messageCount = messageCount;
    }

    public String getEndMessageId() {
        return endMessageId;
    }

    public void setEndMessageId(String endMessageId) {
        this.endMessageId = endMessageId;
    }
}
