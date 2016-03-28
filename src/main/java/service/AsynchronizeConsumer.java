package service;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;

import javax.jms.*;

/**
 * Created by sunyiwei on 2016/3/25.
 */
public class AsynchronizeConsumer extends BaseClass {
    final private static String url = "tcp://localhost:61616";
    final private static String username = "1091";
    final private static String password = "10911091";
    final private static String queueName = "sdywform.queue.1073";

    public static void main(String[] args) throws JMSException {
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(username, password, url);
        Connection connection = connectionFactory.createConnection();
        connection.start();

        //produce
        producer(connection, COUNT);

        //consume
//        consumer(connection);

        //stop
//        connection.stop();
    }

    private static void consumer(Connection connection) throws JMSException {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = new ActiveMQQueue(queueName);
        MessageConsumer consumer = session.createConsumer(destination);

        consumer.setMessageListener(new MessageListener() {
            public void onMessage(Message message) {
                if (message instanceof TextMessage) {
                    try {
                        System.out.println(((TextMessage) message).getText());
                    } catch (JMSException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
    }
}
