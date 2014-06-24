package org.wso2.andes.kernel;

import com.lmax.disruptor.EventFactory;

public class AndesAckData {
    
    public AndesAckData(){}
    
    public AndesAckData(long messageID, String qName, boolean isTopic) {
        super();
        this.messageID = messageID;
        this.qName = qName;
        this.isTopic = isTopic;
    }
    public long messageID; 
    public String qName;
    public boolean isTopic;

    public AndesRemovableMetadata convertToRemovableMetaData() {
        return new AndesRemovableMetadata(this.messageID, this.qName);
    }
    
    public static class AndesAckDataEventFactory implements EventFactory<AndesAckData> {
        @Override
        public AndesAckData newInstance() {
            return new AndesAckData();
        }
    }

    public static EventFactory<AndesAckData> getFactory() {
        return new AndesAckDataEventFactory();
    }

}
