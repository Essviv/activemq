package service;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;

import javax.jms.*;

/**
 * Created by sunyiwei on 2016/3/28.
 */
public class RequestResponse extends BaseClass {
    public static void main(String[] args) throws JMSException {
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(username, password, url);
        Connection connection = connectionFactory.createConnection();
        connection.start();

        consumer(connection);

        produce(connection);
    }

    private static void produce(Connection connection) throws JMSException {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = new ActiveMQQueue(queueName);

        MessageProducer producer = session.createProducer(destination);

        final int COUNT = 100;
        for (int i = 0; i < COUNT; i++) {
            //request
            TextMessage textMessage = session.createTextMessage("Producer: Hi! This is producer!");
            textMessage.setStringProperty("Source", "Producer");
            producer.send(textMessage);

            //wait for response
            MessageConsumer consumer = session.createConsumer(destination, "Source = 'Consumer'");
            Message message = consumer.receive();
            System.out.println("Producer: response is " + ((TextMessage) message).getText());
            consumer.close();
        }

        session.close();
    }

    private static void consumer(Connection connection) throws JMSException {
        final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final Destination destination = new ActiveMQQueue(queueName);
        MessageConsumer consumer = session.createConsumer(destination, "Source = 'Producer'");
        consumer.setMessageListener(new MessageListener() {
            public void onMessage(Message message) {
                try {
                    //receive request
                    System.out.println("Consumer: Received request is " + ((TextMessage) message).getText());
                    System.out.println("Consumer: My response is 'Hi! This is consumer!'");

                    //response
                    MessageProducer producer = session.createProducer(destination);
                    TextMessage textMessage = session.createTextMessage("Hi! This is consumer!");
                    textMessage.setStringProperty("Source", "Consumer");
                    producer.send(textMessage);
                    producer.close();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        });
    }
}
