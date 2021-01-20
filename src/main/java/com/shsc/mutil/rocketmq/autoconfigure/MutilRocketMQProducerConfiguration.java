package com.shsc.mutil.rocketmq.autoconfigure;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MQProducer;
import org.apache.rocketmq.common.UtilAll;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.util.Assert;

import lombok.extern.slf4j.Slf4j;

@Configuration
@EnableConfigurationProperties(MutilProducerRocketMQProperties.class)
@ConditionalOnProperty(prefix = MutilProducerRocketMQProperties.PREFIX, name = "properties", matchIfMissing = true)
@Slf4j
public class MutilRocketMQProducerConfiguration implements InitializingBean,BeanPostProcessor,ApplicationContextAware{
	
	@Autowired
	private MutilProducerRocketMQProperties mutilProducerRocketMQProperties;
	
	private ApplicationContext applicationContext;
	
	private final Map<String,MQProducer> mutilProducer = new ConcurrentHashMap<String, MQProducer>();
	
	@Override
	public void afterPropertiesSet() throws Exception {
		initMutilProducer();
	}
	
	private void initMutilProducer() {
		try {
			Map<String, RocketMQProperties> properties =  mutilProducerRocketMQProperties.getProperties();
			if (properties!=null && properties.size()>0) {
				int index =1;
				for (String key : properties.keySet()) {
					RocketMQProperties rocketMQProperties = properties.get(key);
					//未设置服务节点名称时，默认将key作为名称
					if (StringUtils.isEmpty(rocketMQProperties.getServerName())) rocketMQProperties.setServerName(key);
					validate(rocketMQProperties.getServerName());
					
					//注册实例
				    buildRocketMQProducer(rocketMQProperties);
				    DefaultMQProducer defaultMQProducer = applicationContext.getBean(rocketMQProperties.getServerName(),DefaultMQProducer.class);
				    mutilProducer.put(rocketMQProperties.getServerName(), defaultMQProducer);
				    // 启动Producer实例
				    defaultMQProducer.setNamesrvAddr(rocketMQProperties.getNameServer());
				    defaultMQProducer.setInstanceName(String.valueOf(UtilAll.getPid()+index));
				    defaultMQProducer.setRetryTimesWhenSendAsyncFailed(rocketMQProperties.getProducer().getRetryTimesWhenSendAsyncFailed());
				    defaultMQProducer.setRetryTimesWhenSendFailed(rocketMQProperties.getProducer().getRetryTimesWhenSendFailed());
				    defaultMQProducer.setSendMsgTimeout(rocketMQProperties.getProducer().getSendMessageTimeout());
				    defaultMQProducer.setMaxMessageSize(rocketMQProperties.getProducer().getMaxMessageSize());
				    defaultMQProducer.start();
				    index ++;
				}
			}
		} catch (Exception e) {
			log.error("start rocketmq exception:",e);
		}
	}
	
	private void buildRocketMQProducer(final RocketMQProperties rocketMQProperties){
        try {
        	BeanDefinitionBuilder defaultMQProducerBuiler = BeanDefinitionBuilder.genericBeanDefinition(DefaultMQProducer.class);
        	defaultMQProducerBuiler.addConstructorArgValue(rocketMQProperties.getServerName());
        	defaultMQProducerBuiler.addConstructorArgValue(rocketMQProperties.getProducer().getGroup());
        	defaultMQProducerBuiler.addConstructorArgValue(null);
        	defaultMQProducerBuiler.addConstructorArgValue(rocketMQProperties.getProducer().isEnableMsgTrace());
        	defaultMQProducerBuiler.addConstructorArgValue(rocketMQProperties.getProducer().getCustomizedTraceTopic());
        	BeanDefinition  beanDefinition = defaultMQProducerBuiler.getBeanDefinition();
        	beanDefinition.setAttribute("nameServer", rocketMQProperties.getNameServer());
        	GenericApplicationContext genericApplicationContext = (GenericApplicationContext) applicationContext;
        	genericApplicationContext.registerBeanDefinition(rocketMQProperties.getServerName(), beanDefinition);
        } catch (final Exception e) {
            log.error("registerBeanDefinition rocketmq {} not found!", rocketMQProperties.getServerName());
        }
    }
	
	private void validate (String serverName) {
		MQProducer producer = mutilProducer.get(serverName);
		if (producer!=null) {
			Assert.isTrue(false, "rocketmq producer already exists for "+ serverName);
		}
	}
	
	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	public ApplicationContext getApplicationContext() {
		return applicationContext;
	}
}
