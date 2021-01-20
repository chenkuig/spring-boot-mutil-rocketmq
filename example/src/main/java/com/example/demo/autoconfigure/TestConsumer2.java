package com.example.demo.autoconfigure;

import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Service;

import com.shsc.mutil.rocketmq.autoconfigure.MutilRocketMQMessageListener;

import lombok.extern.slf4j.Slf4j;
@MutilRocketMQMessageListener(topic = "mutil-send-topic", consumerGroup = "my-group-test2",nameServer="127.0.0.1:9876",namespace="mutilMQProducer2")
@Slf4j
@Service
public class TestConsumer2 implements RocketMQListener<String>{

	@Override
	public void onMessage(String message) {
		log.info("my-group-test2接收到的消息:{}",message);
	}
}