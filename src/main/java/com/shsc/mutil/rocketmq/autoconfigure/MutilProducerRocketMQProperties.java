package com.shsc.mutil.rocketmq.autoconfigure;

import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

@ConfigurationProperties(prefix = MutilProducerRocketMQProperties.PREFIX)
public class MutilProducerRocketMQProperties {
	
	public static final String PREFIX = "mutil.rocketmq";
	
	@NestedConfigurationProperty
	private Map<String,RocketMQProperties> properties;


	public Map<String, RocketMQProperties> getProperties() {
		return properties;
	}

	public void setProperties(Map<String, RocketMQProperties> properties) {
		this.properties = properties;
	}
}
