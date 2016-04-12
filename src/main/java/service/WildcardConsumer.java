package service;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;

import javax.jms.*;

/**
 * Created by sunyiwei on 2016/3/28.
 */
public class WildcardConsumer extends BaseClass {
    public static void main(String[] args) throws JMSException {
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(username, password, url);
        Connection connection = connectionFactory.createConnection();
        connection.start();

//        produce(connection, "wildcard.queue.first");

//        produce(connection, "wildcard.queue.second");

//        produce(connection, "wildcard.queue.>");
        produce(connection, ">");

        consume(connection, "wildcard.queue.first");

        consume(connection, "wildcard.queue.second");

//        connection.close();
    }

    private static void produce(final Connection connection, final String queueName) throws JMSException {
        new Thread(new Runnable() {
            public void run() {
                try {
                    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    Destination destination = new ActiveMQQueue(queueName);
                    MessageProducer producer = session.createProducer(destination);

                    final int COUNT = 100;
//                    for (int i = 0; i < COUNT; i++) {
                    while(true){
                        producer.send(session.createTextMessage("Hello world from " + queueName));
                    }

//                    session.close();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    private static void consume(final Connection connection, final String queueName) throws JMSException {
        new Thread(new Runnable() {
            public void run() {
                try {
                    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    Destination destination = new ActiveMQQueue(queueName);
                    MessageConsumer consumer = session.createConsumer(destination);

                    while (true) {
                        Message message = consumer.receive();
                        System.out.println(((TextMessage) message).getText());
                    }
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }
}
