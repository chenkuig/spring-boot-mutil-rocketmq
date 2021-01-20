package com.example.demo.autoconfigure;

import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Service;

import com.shsc.mutil.rocketmq.autoconfigure.MutilRocketMQMessageListener;

import lombok.extern.slf4j.Slf4j;
@MutilRocketMQMessageListener(topic = "mutil-send-topic", consumerGroup = "my-group-test",nameServer="10.110.12.10:9876",namespace="mutilMQProducer1")
@Slf4j
@Service
public class TestConsumer1 implements RocketMQListener<String>{

	@Override
	public void onMessage(String message) {
		log.info("my-group-test接收到的消息:{}",message);
	}
}