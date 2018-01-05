package com.atguigu.recevice;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;

public class TestRecevice2 {
	public static void main(String[] args) {
		try {
			//recevice("tcp://localhost:61616", "message1");
			recevice_topics_message("tcp://localhost:61616", "message2");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void recevice(String url, String application) throws Exception {
		// 创建一个mq的连接对象
		ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(url);
		Connection connection = connectionFactory.createConnection();
		connection.start();

		// 通过连接对象创建一个回话对象
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		Queue queue = session.createQueue(application);

		// 消息的消费者对象通过回话从mq获得消息
		MessageConsumer consumer = session.createConsumer(queue);

		// 消息监听器接收消息
		consumer.setMessageListener(new MessageListener() {
			@Override
			public void onMessage(Message message) {// mq队列中的消息
				// 打印结果
				TextMessage textMessage = (TextMessage) message;
				String text = "";
				try {
					text = textMessage.getText();
				} catch (JMSException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				System.out.println("消费端2对" + text + "进行消费...");
			}
		});
		System.out.println("消费端2启动...");
		// 等待接收消息
		System.in.read();
	}
	
	public static void recevice_topics_message(String url, String application) throws Exception {
		// 创建连接
		ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(url);
		Connection connection = connectionFactory.createConnection();
		connection.setClientID("订阅消费者1");
		connection.start();

		// 创建回话
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		Topic topic = session.createTopic(application);
		MessageConsumer consumer = session.createDurableSubscriber(topic, "订阅消费者1");

		// 创建消费端的监听，获得消息
		consumer.setMessageListener(new MessageListener() {
			@Override
			public void onMessage(Message message) {
				// 打印结果
				TextMessage textMessage = (TextMessage) message;
				String text = "";
				try {
					text = textMessage.getText();
				} catch (JMSException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				System.out.println(text);
			}
		});
		System.out.println("订阅消费者2。。。。");
		// 等待接收消息
		System.in.read();
	}
}
