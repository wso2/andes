package org.wso2.andes.server.smoke;

import javax.jms.JMSException;
import javax.jms.MessageConsumer;


public class CreateRemoveQueueSubscriptionTest extends AbstractJMSClient{
        public CreateRemoveQueueSubscriptionTest(String queueName, int threadIndex) {
            super(queueName);
            this.threadIndex = threadIndex;
        }
        @Override
        public void run() {
        	try {
				MessageConsumer messageConsumer = session.createConsumer(destination);
				messageConsumer.close();
				session.close();
				con.close();
				System.out.println("Done");
			} catch (JMSException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
	
        
        
	
	public static void main(String[] args) {
		CreateRemoveQueueSubscriptionTest test = new CreateRemoveQueueSubscriptionTest("foo", 0);
		test.run();
	}
}
