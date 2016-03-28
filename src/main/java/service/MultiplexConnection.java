package service;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;

import javax.jms.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by sunyiwei on 2016/3/25.
 */
public class MultiplexConnection extends BaseClass{
    public static void main(String[] args) throws JMSException {
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(username, password, url);
        Connection connection = connectionFactory.createConnection();
        connection.start();

        final int THREAD_COUNT = 3;

        System.out.println("Producer start to work");
        ExecutorService executors = Executors.newFixedThreadPool(THREAD_COUNT);
        producer(executors, connection);
        stop(executors);
        System.out.println("Producer has completed its work");

        System.out.println("Consumer start to work");
        executors = Executors.newFixedThreadPool(THREAD_COUNT);
        consumer(executors, connection);
        stop(executors);
        System.out.println("Consumer has consumed all messages");

        connection.close();
    }

    private static void producer(ExecutorService executors, Connection connection) throws JMSException {
        final int THREAD_COUNT = 3;
        for (int i = 0; i < THREAD_COUNT; i++) {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = new ActiveMQQueue(queueName);

            final int INDEX = i;
            final MessageProducer producer = session.createProducer(destination);

            executors.submit(new Runnable() {
                public void run() {
                    final int COUNT = 100;
                    for (int j = INDEX * COUNT; j < (INDEX + 1) * COUNT; j++) {
                        try {
                            TextMessage message = new ActiveMQTextMessage();
                            message.setText("hello world from " + j);
                            producer.send(message);
                        } catch (JMSException e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
        }
    }

    private static void consumer(ExecutorService executors, final Connection connection) {
        final int COUNT = 3;
        for (int i = 0; i < COUNT; i++) {
            executors.submit(new Runnable() {
                public void run() {
                    try {
                        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                        Destination destination = new ActiveMQQueue(queueName);

                        final MessageConsumer consumer = session.createConsumer(destination);

                        while (true) {
                            Message message = consumer.receive();
                            if (message instanceof TextMessage) {
                                System.out.format("%s: %s. %n", Thread.currentThread().getName(), ((TextMessage) message).getText());
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
