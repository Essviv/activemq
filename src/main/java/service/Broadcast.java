package service;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.ActiveMQTopic;

import javax.jms.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by sunyiwei on 2016/3/25.
 */
public class Broadcast {
    final private static String url = "tcp://localhost:61616";
    final private static String username = "1091";
    final private static String password = "10911091";
    final private static String queueName = "sdywform.queue.1073";

    public static void main(String[] args) throws JMSException {
        final int COUNT = 10;
        final String TOPIC_NAME = "Broadcast";

        ExecutorService consumers = Executors.newFixedThreadPool(COUNT);
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(username, password, url);
        Connection connection = connectionFactory.createConnection();
        connection.start();

        //consumer
        consumer(consumers, connection, TOPIC_NAME, COUNT);

        //sleep for a while
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        //producer
        producer(connection, TOPIC_NAME, COUNT);

        //stop
//        stop(consumers);

        //close
//        connection.close();
    }

    private static void producer(Connection connection, final String topicName, int count) throws JMSException {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createTopic(topicName);

        Destination destination = new ActiveMQTopic(topicName);
        MessageProducer producer = session.createProducer(destination);
        for (int i = 0; i < count; i++) {
            TextMessage textMessage = new ActiveMQTextMessage();
            textMessage.setText("Hello world from " + i);
            producer.send(textMessage);
        }

        session.close();
    }

    private static void consumer(ExecutorService consumers, final Connection connection, final String topicName, int count) {
        for (int i = 0; i < count; i++) {
            final int INDEX = i;
            consumers.submit(new Runnable() {
                public void run() {
                    try {
                        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                        session.createTopic(topicName);
                        Destination destination = new ActiveMQTopic(topicName);

                        MessageConsumer consumer = session.createConsumer(destination);
                        while (true) {
                            Message message = consumer.receive();
                            if (message instanceof TextMessage) {
                                System.out.format("%s receives %s. %n", Thread.currentThread().getName(), ((TextMessage) message).getText());
                            }
                        }
                    } catch (JMSException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
    }
}
