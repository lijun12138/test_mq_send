package com.atguigu.send;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;

public class TestMqSend {
	public static void main(String[] args) {
		try {
			//send_queues_message("记得还钱", "message1", "tcp://localhost:61616");
			send_topics_message("记得还钱2", "message2", "tcp://localhost:61616");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void send_queues_message(String message, String application, String url) throws JMSException {
		// 创建一个mq的连接对象
		ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(url);
		Connection connection = connectionFactory.createConnection();
		connection.start();

		// 通过连接对象创建一个回话对象
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		Queue queue = session.createQueue(application);

		// 消息生产者producer(消息发布的执行者)对象通过回话向mq发送消息
		MessageProducer producer = session.createProducer(queue);
		TextMessage textMessage = session.createTextMessage(message);
		producer.send(textMessage);

		// 关闭资源连接
		producer.close();
		session.close();
		connection.close();

	}

	public static void send_topics_message(String message, String application, String url) throws Exception {
		// 创建连接对象
		ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(url);
		Connection connection = connectionFactory.createConnection();
		connection.start();

		// 创建session
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		Topic topic = session.createTopic(application);

		// 通过producer执行消息的推送
		MessageProducer producer = session.createProducer(topic);
		TextMessage textMessage = session.createTextMessage(message);
		producer.send(textMessage);
		producer.close();
		session.close();
		connection.close();
	}

}
