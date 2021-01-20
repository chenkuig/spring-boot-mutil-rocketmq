package com.example.demo.autoconfigure;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import lombok.extern.slf4j.Slf4j;

@RestController
@RequestMapping("/test")
@Slf4j
public class TestProducer{
	
	@Autowired
	@Qualifier("mutilMQProducer1")
	private DefaultMQProducer mutilMQProducer1;
	
	@Autowired
	@Qualifier("mutilMQProducer2")
	private DefaultMQProducer mutilMQProducer2;
	
	//调用地址：http://127.0.0.1:8071/test/sendMessage
	@GetMapping("/sendMessage")
	void sendTest() {
		try {
			Message message= new Message();
			message.setTopic("mutil-send-topic");
			message.setBody("这是一个mutil消息".getBytes());
			SendResult sendResult = mutilMQProducer1.send(message);
			log.info("sendResult:{}",sendResult.getSendStatus());
			
			
			Message message2= new Message();
			message2.setTopic("mutil-send-topic");
			message2.setBody("这是一个mutil消息2".getBytes());
			SendResult sendResult2 = mutilMQProducer2.send(message2);
			log.info("sendResult2:{}",sendResult2.getSendStatus());
		} catch (Exception e) {
			log.error("",e);
		}
	}
}
