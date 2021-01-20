# spring-boot-mutil-rocketmq

实现了rocketmq多个客户端实例的  
如下配置 

#rocketmq实例一
mutil.rocketmq.properties.service1.serverName =  mutilMQProducer1
mutil.rocketmq.properties.service1.nameServer=10.110.12.10:9876
mutil.rocketmq.properties.service1.producer.group=my-group-test

#rocketmq实例二
mutil.rocketmq.properties.service2.serverName =  mutilMQProducer2
mutil.rocketmq.properties.service2.nameServer=127.0.0.1:9876
mutil.rocketmq.properties.service2.producer.group=my-group-test2
