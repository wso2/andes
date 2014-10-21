package org.wso2.andes.server.smoke;


public class CreateRemoveTopicSubscriptionTest extends AbstractJMSTopicClient{
        public CreateRemoveTopicSubscriptionTest(String queueName, int threadIndex) {
            super(queueName);
            this.threadIndex = threadIndex;
        }
        @Override
        public void run() {
            try {
                // start the connection
                topicConnection.start();

                // create a topic subscriber
                javax.jms.TopicSubscriber topicSubscriber = topicSession.createSubscriber(topic);

                topicSubscriber.close();
                topicSession.close();
                System.out.println("Done");
            }catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
	
        
        
	
	public static void main(String[] args) {
		CreateRemoveTopicSubscriptionTest test = new CreateRemoveTopicSubscriptionTest("foo", 0);
		test.run();
	}
}
