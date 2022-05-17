# Reactive Kafka操作

如果需要使用kafka首先需要在 common模块的pom.xml文件中加入如下maven坐标

```xml
<dependency>
 <groupId>com.g7.framework</groupId>
 <artifactId>kafka-reactive-spring-boot-autoconfigure</artifactId>
</dependency>
```

## Kafka消息发送方式一

```java
/**
 * @author dreamyao
 * @title
 * @date 2022/2/2 4:35 下午
 * @since 1.0.0
 */
@Service
public class OpLogBiz {
 
    private static final Logger logger = LoggerFactory.getLogger(OpLogBiz.class);
    private final KafkaSender<String, String> kafkaSender;
    private final DefaultUidGenerator defaultUidGenerator;
 
    public OpLogBiz(KafkaSender<String, String> kafkaSender,
                    DefaultUidGenerator defaultUidGenerator) {
        this.kafkaSender = kafkaSender;
        this.defaultUidGenerator = defaultUidGenerator;
    }
 
    /**
     * 发送操作数据
     * @param messageId 消息ID
     */
    public void sendOpLogMessage(String messageId) {
        final Mono<SenderRecord<String, String, Long>> record = findOpLogDoc8MessageId(messageId)
                .map(log -> SenderRecord.create(new ProducerRecord<>("test_spring_cloud_stream",
                        JsonUtils.toJson(log)), defaultUidGenerator.getUid()));
 
        kafkaSender.send(record)
                .doOnError(throwable -> logger.error("Send failed", throwable))
                .subscribe();
    }
}

```

## Kafka消息发送方式二

```java
@Autowired
private ReactiveKafkaTemplate reactiveKafkaTemplate
 
reactiveKafkaTemplate.send("test_ntocc_gmq_topic","value").subscribe()
```

## Kafka消息消费

消费逻辑编写

```java
/**
 * @author dreamyao
 * @title
 * @date 2022/1/29 9:54 下午
 * @since 1.0.0
 */
@Component
public class StreamDefaultMessageComsumer implements AbstractMessageComsumer {
 
    private static final Logger logger = LoggerFactory.getLogger(StreamDefaultMessageComsumer.class);
 
    @Override
    public Mono<?> consume(ConsumerRecord<String, String> record) {
        final String value = record.value();
        logger.info("消费到的消息：{}", value);
    }
}
```

创建消费者

```java
/**
 * @author dreamyao
 * @title
 * @date 2022/2/2 4:32 下午
 * @since 1.0.0
 */
@Configuration
public class ReactiveReceiverContainerConfiguration {
 
    private final StreamDefaultMessageComsumer streamDefaultMessageComsumer;
 
    public ReactiveReceiverContainerConfiguration(StreamDefaultMessageComsumer streamDefaultMessageComsumer) {
        this.streamDefaultMessageComsumer = streamDefaultMessageComsumer;
    }
 
    @Bean(name = "kafkaReceiverContainer")
    public ReactiveKafkaReceiverContainer kafkaReceiverContainer() {
        return new ReactiveKafkaReceiverContainer(
                streamDefaultMessageComsumer,
                "ntocc-reactor-demo",
                "test_spring_cloud_stream");
    }
}
```