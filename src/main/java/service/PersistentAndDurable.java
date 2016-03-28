package service;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * non-persistent msg + non-durable subscriber
 * <p>
 * Created by sunyiwei on 2016/3/27.
 */
public class PersistentAndDurable extends BaseClass {
    public static void main(String[] args) throws JMSException {
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(username, password, url);
        Connection connection = connectionFactory.createConnection();
        connection.setClientID("DurableTopicClient");
        connection.start();

        final String TOPIC_NAME = "TOPIC";

        consumer(connection, TOPIC_NAME, "Durable Subscriber 1");
////
//        //sleep for a while
//        try {
//            Thread.sleep(3000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }

//        producer
//        producer(connection, TOPIC_NAME);

////        sleep for a while
//        try {
//            Thread.sleep(1000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//
//        //consumer
        consumer(connection, TOPIC_NAME, "Durable Subscriber 2");

//        connection.close();
    }

    private static void consumer(final Connection connection, final String topicName, final String subscriberName) throws JMSException {
        final int COUNT = 1;
        ExecutorService executors = Executors.newFixedThreadPool(COUNT, new ThreadFactory() {
            public Thread newThread(Runnable r) {
                return new Thread(r, "consumer");
            }
        });

        for (int i = 0; i < COUNT; i++) {
            executors.submit(new Runnable() {
                public void run() {
                    Session session = null;
                    try {
                        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

                        Topic topic = session.createTopic(topicName);
                        TopicSubscriber subscriber = session.createDurableSubscriber(topic, subscriberName);

                        while (true) {
                            Message message = subscriber.receive();
                            if (message instanceof TextMessage) {
                                System.out.format("%s: %s. %n", Thread.currentThread().getName(), ((TextMessage) message).getText());
                            }
                        }
                    } catch (JMSException e) {
                        e.printStackTrace();
                    } finally {
                        if (session != null) {
                            try {
                                session.close();
                            } catch (JMSException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
            });
        }
    }

    private static void producer(final Connection connection, final String topicName) throws JMSException {
        new Thread(new Runnable() {
            public void run() {
                Session session = null;
                try {
                    session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

                    final int COUNT = 10;

                    Destination destination = session.createTopic(topicName);
                    MessageProducer producer = session.createProducer(destination);
//                    producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
                    while (true) {
                        Thread.sleep(100);
                        producer.send(session.createTextMessage("Non-persistent topic"), DeliveryMode.PERSISTENT, 4, 10000);
                    }
                } catch (JMSException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    if (session != null) {
                        try {
                            session.close();
                        } catch (JMSException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }, "producer").start();
    }
}
