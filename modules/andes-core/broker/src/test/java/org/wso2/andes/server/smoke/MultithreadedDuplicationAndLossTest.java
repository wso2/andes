package org.wso2.andes.server.smoke;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.TextMessage;

public class MultithreadedDuplicationAndLossTest extends MultiThreadedTest {
    final ConcurrentLinkedQueue<Long> sentMessages = new ConcurrentLinkedQueue<Long>();
    final ConcurrentLinkedQueue<Long> receivedMessage = new ConcurrentLinkedQueue<Long>();

    @Override
    protected TextMessage createMessage(int threadIndex, int i, Session session, int globalMsgIndex)
            throws JMSException {
        sentMessages.add((long) globalMsgIndex);
        return session.createTextMessage(String.valueOf(globalMsgIndex));
    }

    @Override
    protected void postProcessesReceivedMessage(int threadIndex, int i, TextMessage message, int globalMsgReceivedIndex) {
        try {
            receivedMessage.add(Long.valueOf(message.getText()));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * @param args
     * @throws InterruptedException
     */
    public static void main(String[] args) throws Exception {
        MultithreadedDuplicationAndLossTest base = new MultithreadedDuplicationAndLossTest();
        int producerCount = 2;
        int consumerCount = 2;
        int queueCount = 30; 

        for(int j=0;j<queueCount;j++){
            String queueName = "queue"+j;
            for (int i = 0; i < consumerCount; i++) {
                new Thread(base.new SimpleConsumer(queueName, i)).start();
            }
            Thread.sleep(10000);

            for (int i = 0; i < producerCount; i++) {
                new Thread(base.new SimpleProducer(queueName, i)).start();
            }
        }

        System.out.println("Enter any key when the test is done");
        System.in.read();

        Set<Long> uniqueReceivedItems = new HashSet<Long>(base.receivedMessage);
        if (uniqueReceivedItems.size() != base.receivedMessage.size()) {
            System.out.println("there are duplicated values, received " + base.receivedMessage.size() + " != "
                    + "unique items" + uniqueReceivedItems.size());
            
            List<Long> duplicateMessages = new ArrayList<Long>();
            duplicateMessages.addAll(base.receivedMessage);
            for(Long val: uniqueReceivedItems){
                duplicateMessages.remove(val);
            }
            System.out.println("duplicate items are " + duplicateMessages);
        }

        if (uniqueReceivedItems.size() != base.sentMessages.size()) {
            List<Long> missingMessages = new ArrayList<Long>();
            missingMessages.addAll(base.sentMessages);
            missingMessages.removeAll(uniqueReceivedItems);
            System.out.println("Some messages are missing, received " + uniqueReceivedItems.size() + "!=  sent " + base.sentMessages.size());
            System.out.println("Missing messages are "+ missingMessages);
        }
        System.out.println("Test completed");

    }

}
