package org.wso2.andes.server.cassandra;

import java.io.Serializable;

public class Slot implements Serializable {

    private long messageCount;
    private long startMessageId;
    private long endMessageId;
    private String queue;


    public void setQueue(String queue) {
        this.queue = queue;
    }

    public String getQueue() {
        return queue;
    }

    public long getMessageCount() {
        return messageCount;
    }

    public void setMessageCount(long messageCount) {
        this.messageCount = messageCount;
    }

    public long getEndMessageId() {
        return endMessageId;
    }

    public void setEndMessageId(long endMessageId) {
        this.endMessageId = endMessageId;
    }

    public long getStartMessageId() {
        return startMessageId;
    }

    public void setStartMessageId(long startMessageId) {
        this.startMessageId = startMessageId;
    }

    public void setMessageRange(long startMessageId, long endMessageId){
       this.startMessageId = startMessageId;
        this.endMessageId = endMessageId;
    }

}
