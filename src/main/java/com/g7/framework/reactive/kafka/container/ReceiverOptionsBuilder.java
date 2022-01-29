package com.g7.framework.reactive.kafka.container;

import com.g7.framework.reactive.kafka.properties.KafkaProperties;
import com.g7.framework.reactive.kafka.util.ReadPropertiesUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.util.StringUtils;
import reactor.kafka.receiver.ReceiverOptions;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * @author dreamyao
 * @title
 * @date 2022/1/27 11:00 下午
 * @since 1.0.0
 */
public class ReceiverOptionsBuilder<K,V> {

    private final KafkaProperties properties;
    private final String[] topics;
    private final String groupId;

    public ReceiverOptionsBuilder(KafkaProperties properties, String[] topics, String groupId) {
        this.properties = properties;
        this.topics = topics;
        this.groupId = groupId;
    }

    public ReceiverOptions<K, V> build() {
        return createKafkaReceiver();
    }

    private ReceiverOptions<K, V> createKafkaReceiver() {
        Properties configs = ReadPropertiesUtils.readConsumerDefaultProperties();
        configs.setProperty("bootstrap.servers", properties.getBootstrap().getServers());
        configs.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        getConsumerDeserializer(configs);

        return createKafkaReceiver(configs);
    }

    private ReceiverOptions<K, V> createKafkaReceiver(Properties configs) {
        return ReceiverOptions.<K,V>create(configs)
                .closeTimeout(Duration.ofSeconds(30))
                .commitInterval(Duration.ZERO)//禁用定期提交
                .commitBatchSize(0)//按批次大小禁用提交
                .pollTimeout(Duration.ofMillis(100))
                .subscription(Arrays.asList(topics));
    }

    private void getConsumerDeserializer(Properties consumerDefaultProperties) {
        String keyDeserializer = properties.getConsumer().getKeyDeserializer();
        if (Boolean.FALSE.equals(StringUtils.isEmpty(keyDeserializer))) {
            consumerDefaultProperties.setProperty("key.deserializer", keyDeserializer);
        }

        String valueDeserializer = properties.getConsumer().getValueDeserializer();
        if (Boolean.FALSE.equals(StringUtils.isEmpty(valueDeserializer))) {
            consumerDefaultProperties.setProperty("value.deserializer", valueDeserializer);
        }
    }
}
