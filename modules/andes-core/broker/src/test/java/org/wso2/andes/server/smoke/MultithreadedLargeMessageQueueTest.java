package org.wso2.andes.server.smoke;

import java.util.Random;

import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.TextMessage;

public class MultithreadedLargeMessageQueueTest extends MultiThreadedTest{
    private int messageSize = 164*1024; ;
    //private int messageSize = 10*1024*1024; ;
    Random random = new Random();
    
    
    
    private String message; 
    private MultithreadedLargeMessageQueueTest() {
        char[] buf = new char[messageSize];
        for(int i =0;i< buf.length;i++){
            //buf[i] = (char)random.nextInt(1024);
            buf[i] = 'A';
        }
        
        message = new String(buf); 
        messageCountPerThread = 100;
    }
    @Override
    protected TextMessage createMessage(int threadIndex, int i, Session session, int globalMsgIndex)
            throws JMSException {
        String msgAsStr = threadIndex+ ":" +String.valueOf(i) + "#" + message;
        return session.createTextMessage(msgAsStr);
    }
    @Override
    protected void postProcessesReceivedMessage(int threadIndex, int i, TextMessage message, int globalMsgReceivedIndex) {
        try {
            if(message.getText().length() < this.message.length()){
                System.out.println("Some data missing in the message ="+ message.getText().length());
            }
        } catch (JMSException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }
    
    
    /**
     * @param args
     * @throws InterruptedException 
     */
    public static void main(String[] args) throws Exception {
        MultithreadedLargeMessageQueueTest base = new MultithreadedLargeMessageQueueTest();
        int producerCount = 10; 
        int consumerCount = 10; 
        String queueName = "queue1";

        for(int i = 0;i< consumerCount;i++){
            new Thread(base.new SimpleConsumer(queueName, i)).start(); 
        }
        Thread.sleep(10000);
        
        for(int i = 0;i< producerCount;i++){
            new Thread(base.new SimpleProducer(queueName, i)).start();
        }
    }

   
}
