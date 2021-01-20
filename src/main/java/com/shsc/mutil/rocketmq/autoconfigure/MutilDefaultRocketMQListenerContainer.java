package com.shsc.mutil.rocketmq.autoconfigure;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Objects;

import org.apache.rocketmq.client.AccessChannel;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.spring.annotation.ConsumeMode;
import org.apache.rocketmq.spring.annotation.MessageModel;
import org.apache.rocketmq.spring.annotation.SelectorType;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.apache.rocketmq.spring.core.RocketMQPushConsumerLifecycleListener;
import org.apache.rocketmq.spring.support.DefaultRocketMQListenerContainer;
import org.apache.rocketmq.spring.support.RocketMQUtil;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MutilDefaultRocketMQListenerContainer extends DefaultRocketMQListenerContainer implements ApplicationContextAware{
	
    private AccessChannel accessChannel = AccessChannel.LOCAL;
	
    private long consumeTimeout;
    
    private String name;
    
    private String namespace;
    
	private MutilRocketMQMessageListener mutilRocketMQMessageListener;
	
	private ApplicationContext applicationContext;
	
	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		super.setApplicationContext(applicationContext);
		this.applicationContext  = applicationContext;
	}

	public MutilRocketMQMessageListener getMutilRocketMQMessageListener() {
		return mutilRocketMQMessageListener;
	}

	public void setMutilRocketMQMessageListener(MutilRocketMQMessageListener anno) {
		this.mutilRocketMQMessageListener = anno;
        setValueToField("consumeMode", anno.consumeMode());
        setValueToField("consumeThreadMax", anno.consumeThreadMax());
        setValueToField("messageModel", anno.messageModel());
        setValueToField("selectorExpression", anno.selectorExpression());
        setValueToField("selectorType", anno.selectorType());
        setValueToField("consumeTimeout", anno.consumeTimeout());
        this.namespace = anno.namespace();
        this.consumeTimeout = anno.consumeTimeout();
		this.mutilRocketMQMessageListener = mutilRocketMQMessageListener;
	}
	
	
	private void setValueToField (String name,Object value) {
		Field field = ReflectionUtils.findField(this.getClass(), name);
		if (field!=null) {
			field.setAccessible(true);
			ReflectionUtils.setField(field, this, value);
		}
	}
	
	@Override
	public void afterPropertiesSet() throws Exception {
		initRocketMQPushConsumer();
		Method method = ReflectionUtils.findMethod(this.getClass(), "getMessageType");
		method.setAccessible(true);
		Class messageType = (Class) ReflectionUtils.invokeMethod(method, this);
		setValueToField("messageType",messageType);
	}
	
	private void initRocketMQPushConsumer() throws MQClientException {
		RocketMQListener rocketMQListener = super.getRocketMQListener();
		ConsumeMode consumeMode = super.getConsumeMode();
		SelectorType selectorType  = super.getSelectorType();
		String selectorExpression = super.getSelectorExpression();
		MessageModel messageModel = super.getMessageModel();
		String nameServer = super.getNameServer();
		String consumerGroup = super.getConsumerGroup();
		String topic = super.getTopic();
		int consumeThreadMax = super.getConsumeThreadMax();
        Assert.notNull(rocketMQListener, "Property 'rocketMQListener' is required");
        Assert.notNull(consumerGroup, "Property 'consumerGroup' is required");
        Assert.notNull(nameServer, "Property 'nameServer' is required");
        Assert.notNull(topic, "Property 'topic' is required");
        RPCHook rpcHook = RocketMQUtil.getRPCHookByAkSk(applicationContext.getEnvironment(),this.mutilRocketMQMessageListener.accessKey(), this.mutilRocketMQMessageListener.secretKey());
        boolean enableMsgTrace = mutilRocketMQMessageListener.enableMsgTrace();
        DefaultMQPushConsumer consumer = null;
        if (Objects.nonNull(rpcHook)) {
            consumer = new DefaultMQPushConsumer(namespace,consumerGroup, rpcHook, new AllocateMessageQueueAveragely(),
                enableMsgTrace, this.applicationContext.getEnvironment().
                resolveRequiredPlaceholders(this.mutilRocketMQMessageListener.customizedTraceTopic()));
            consumer.setVipChannelEnabled(false);
            consumer.setInstanceName(RocketMQUtil.getInstanceName(rpcHook, consumerGroup));
        } else {
            log.debug("Access-key or secret-key not configure in " + this + ".");
            consumer = new DefaultMQPushConsumer(namespace,consumerGroup,null,new AllocateMessageQueueAveragely(),enableMsgTrace,
                    this.applicationContext.getEnvironment().
                    resolveRequiredPlaceholders(this.mutilRocketMQMessageListener.customizedTraceTopic()));
        }

        String customizedNameServer = this.applicationContext.getEnvironment().resolveRequiredPlaceholders(this.mutilRocketMQMessageListener.nameServer());
        if (customizedNameServer != null) {
            consumer.setNamesrvAddr(customizedNameServer);
        } else {
            consumer.setNamesrvAddr(nameServer);
        }
        if (accessChannel != null) {
            consumer.setAccessChannel(accessChannel);
        }
        consumer.setConsumeThreadMax(consumeThreadMax);
        if (consumeThreadMax < consumer.getConsumeThreadMin()) {
            consumer.setConsumeThreadMin(consumeThreadMax);
        }
        consumer.setConsumeTimeout(consumeTimeout);
        consumer.setInstanceName(this.name);
        super.setConsumer(consumer);
        switch (messageModel) {
            case BROADCASTING:
                consumer.setMessageModel(org.apache.rocketmq.common.protocol.heartbeat.MessageModel.BROADCASTING);
                break;
            case CLUSTERING:
                consumer.setMessageModel(org.apache.rocketmq.common.protocol.heartbeat.MessageModel.CLUSTERING);
                break;
            default:
                throw new IllegalArgumentException("Property 'messageModel' was wrong.");
        }

        switch (selectorType) {
            case TAG:
                consumer.subscribe(topic, selectorExpression);
                break;
            case SQL92:
                consumer.subscribe(topic, MessageSelector.bySql(selectorExpression));
                break;
            default:
                throw new IllegalArgumentException("Property 'selectorType' was wrong.");
        }

        switch (consumeMode) {
            case ORDERLY:
                consumer.setMessageListener(new DefaultMessageListenerOrderly());
                break;
            case CONCURRENTLY:
                consumer.setMessageListener(new DefaultMessageListenerConcurrently());
                break;
            default:
                throw new IllegalArgumentException("Property 'consumeMode' was wrong.");
        }

        if (rocketMQListener instanceof RocketMQPushConsumerLifecycleListener) {
            ((RocketMQPushConsumerLifecycleListener) rocketMQListener).prepareStart(consumer);
        }
    }
	
	@Override
	public void setName(String name) {
		super.setName(name);
		this.name = name;
	}

	public String getNamespace() {
		return namespace;
	}

	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}
}
