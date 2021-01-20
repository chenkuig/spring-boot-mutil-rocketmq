package com.shsc.mutil.rocketmq.autoconfigure;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.rocketmq.client.AccessChannel;
import org.apache.rocketmq.spring.annotation.ConsumeMode;
import org.apache.rocketmq.spring.annotation.MessageModel;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.apache.rocketmq.spring.support.DefaultRocketMQListenerContainer;
import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.support.BeanDefinitionValidationException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.util.StringUtils;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;
@Slf4j
@Configuration
public class MutilListenerContainerConfiguration implements SmartInitializingSingleton,ApplicationContextAware{
	private ConfigurableApplicationContext applicationContext;
	
	private AtomicLong counter = new AtomicLong(0);
	
    private StandardEnvironment environment;
	
    private ObjectMapper objectMapper;
    
    public MutilListenerContainerConfiguration(ObjectMapper rocketMQMessageObjectMapper,
            StandardEnvironment environment) {
            this.objectMapper = rocketMQMessageObjectMapper;
            this.environment = environment;
        }
    
	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = (ConfigurableApplicationContext) applicationContext;
	}

	@Override
	public void afterSingletonsInstantiated() {
		 Map<String, Object> beans = this.applicationContext.getBeansWithAnnotation(MutilRocketMQMessageListener.class);
	     if (Objects.nonNull(beans)) {
	            beans.forEach(this::registerContainer);
	     }
	}
	
	private void registerContainer(String beanName, Object bean) {
        Class<?> clazz = AopProxyUtils.ultimateTargetClass(bean);

        if (!RocketMQListener.class.isAssignableFrom(bean.getClass())) {
            throw new IllegalStateException(clazz + " is not instance of " + RocketMQListener.class.getName());
        }

        MutilRocketMQMessageListener annotation = clazz.getAnnotation(MutilRocketMQMessageListener.class);
        validate(annotation);

        String containerBeanName = String.format("%s_%s", DefaultRocketMQListenerContainer.class.getName(),
            counter.incrementAndGet());
        GenericApplicationContext genericApplicationContext = (GenericApplicationContext) applicationContext;

        genericApplicationContext.registerBean(containerBeanName, DefaultRocketMQListenerContainer.class,
            () -> createRocketMQListenerContainer(containerBeanName, bean, annotation));
        DefaultRocketMQListenerContainer container = genericApplicationContext.getBean(containerBeanName,
            DefaultRocketMQListenerContainer.class);
        if (!container.isRunning()) {
            try {
                container.start();
            } catch (Exception e) {
                log.error("Started container failed. {}", container, e);
                throw new RuntimeException(e);
            }
        }

        log.info("Register the listener to container, listenerBeanName:{}, containerBeanName:{}", beanName, containerBeanName);
    }
	
	
	private DefaultRocketMQListenerContainer createRocketMQListenerContainer(String name, Object bean, MutilRocketMQMessageListener annotation) {
		MutilDefaultRocketMQListenerContainer container = new MutilDefaultRocketMQListenerContainer();

        String nameServer = environment.resolvePlaceholders(annotation.nameServer());
        String accessChannel = environment.resolvePlaceholders(annotation.accessChannel());
        container.setNameServer(nameServer);
        if (!StringUtils.isEmpty(accessChannel)) {
            container.setAccessChannel(AccessChannel.valueOf(accessChannel));
        }
        container.setNamespace(environment.resolvePlaceholders(annotation.namespace()));
        container.setTopic(environment.resolvePlaceholders(annotation.topic()));
        container.setConsumerGroup(environment.resolvePlaceholders(annotation.consumerGroup()));
        container.setMutilRocketMQMessageListener(annotation);
        container.setObjectMapper(objectMapper);
        container.setRocketMQListener((RocketMQListener) bean);
        container.setName(name);  // REVIEW ME, use the same clientId or multiple?

        return container;
    }
	
    private void validate(MutilRocketMQMessageListener annotation) {
        if (annotation.consumeMode() == ConsumeMode.ORDERLY &&
            annotation.messageModel() == MessageModel.BROADCASTING) {
            throw new BeanDefinitionValidationException(
                "Bad annotation definition in @MutilRocketMQMessageListener, messageModel BROADCASTING does not support ORDERLY message!");
        }
        String nameServer = environment.resolvePlaceholders(annotation.nameServer());
        if(StringUtils.isEmpty(nameServer)){
        	throw new BeanDefinitionValidationException(
                    "Bad annotation definition in @MutilRocketMQMessageListener, nameServer is required!");
        }
        String namespace = environment.resolvePlaceholders(annotation.namespace());
        if(StringUtils.isEmpty(namespace)){
        	throw new BeanDefinitionValidationException(
                    "Bad annotation definition in @MutilRocketMQMessageListener, namespace is required!");
        }
    }
}
