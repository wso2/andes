package org.wso2.andes.server.smoke;

import java.util.Random;

import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.TextMessage;

public class MultiThreadedLargeMessageTopicTest extends MultiThreadedTopicTest{
    private int messageSize = 164*1024; ;
    Random random = new Random();
    
    
    
    private String message; 
    private MultiThreadedLargeMessageTopicTest() {
        byte[] buf = new byte[messageSize];
        for(int i =0;i< buf.length;i++){
            buf[i] = 'A';
        }
        
        message = new String(buf); 
        messageCountPerThread = 100;
    }
    
    protected TextMessage createMessage(int threadIndex, int i, Session session, int globalMsgIndex) throws JMSException{
        String messageAsStr = threadIndex+","+i+"#" +message;
        System.out.println("size="+messageAsStr.length());
        return session.createTextMessage(messageAsStr);
    }
    
    protected void postProcessesReceivedMessage(int threadIndex, int i, TextMessage message, int globalMsgReceivedIndex){
        System.out.println();

    }
    
    /**
     * @param args
     * @throws InterruptedException 
     */
    public static void main(String[] args) throws Exception {
        System.setProperty(org.wso2.andes.configuration.ClientProperties.AMQP_VERSION, "0-91");
        System.setProperty("qpid.dest_syntax" , "BURL");
        
        
        MultiThreadedLargeMessageTopicTest base = new MultiThreadedLargeMessageTopicTest();
        int producerCount = 1; 
        int consumerCount = 5; 
        String queueName = "topic1";

        for(int i = 0;i< consumerCount;i++){
            new Thread(base.new SimpleConsumer(queueName, i)).start(); 
        }
        Thread.sleep(10000);
        
        for(int i = 0;i< producerCount;i++){
            new Thread(base.new SimpleProducer(queueName, i)).start();
        }
    }
}
