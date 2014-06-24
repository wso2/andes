package org.wso2.andes.kernel.distrupter;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.AndesAckData;
import org.wso2.andes.kernel.MessageStore;

import com.lmax.disruptor.EventHandler;

/**
 * We do this to make Listener take turns while running. So we can run many copies of these and control number
 * of IO threads through that.
 */

public class AckHandler implements EventHandler<AndesAckData> {
    private MessageStore messageStore;
    private List<AndesAckData> ackList = new ArrayList<AndesAckData>();

    public AckHandler(MessageStore messageStore) {
        this.messageStore = messageStore;
    }

    public void onEvent(final AndesAckData event, final long sequence, final boolean endOfBatch) throws Exception {
        ackList.add(event);
        if (endOfBatch) {
            messageStore.ackReceived(ackList);
            ackList.clear();
        }
    }
}
