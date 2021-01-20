package com.shsc.mutil.rocketmq.autoconfigure;

import org.apache.rocketmq.spring.core.RocketMQListener;
import org.apache.rocketmq.spring.support.DefaultRocketMQListenerContainer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@ConditionalOnClass({DefaultRocketMQListenerContainer.class,RocketMQListener.class})
@Configuration
@EnableConfigurationProperties(MutilProducerRocketMQProperties.class)
@Import({MutilRocketMQProducerConfiguration.class,MutilListenerContainerConfiguration.class})
public class MutilRocketMQAutoConfig{
	
}
