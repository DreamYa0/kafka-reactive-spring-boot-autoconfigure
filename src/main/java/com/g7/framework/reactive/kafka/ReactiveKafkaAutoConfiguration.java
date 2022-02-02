package com.g7.framework.reactive.kafka;

import com.g7.framework.reactive.kafka.listener.ShutdownHookListener;
import com.g7.framework.reactive.kafka.properties.KafkaProperties;
import com.g7.framework.reactive.kafka.util.ReadPropertiesUtils;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.util.StringUtils;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

import java.time.Duration;
import java.util.Properties;

/**
 * @author dreamyao
 * @title
 * @date 2018/8/5 上午11:39
 * @since 1.0.0
 */
@EnableConfigurationProperties(KafkaProperties.class)
public class ReactiveKafkaAutoConfiguration {

    private final KafkaProperties properties;

    public ReactiveKafkaAutoConfiguration(KafkaProperties properties) {
        this.properties = properties;
    }

    @Bean
    @ConditionalOnMissingBean(value = KafkaSender.class)
    public KafkaSender<String,String> kafkaSender() {
        Properties properties = buildProducerProperties();
        final SenderOptions<String, String> options = SenderOptions.<String,String>create(properties)
                .maxInFlight(1000)
                .closeTimeout(Duration.ofSeconds(30))
                .scheduler(Schedulers.boundedElastic());
        return KafkaSender.create(options);
    }

    @Bean
    @ConditionalOnMissingBean(value = ShutdownHookListener.class)
    public ShutdownHookListener shutdownHookListener(KafkaSender<String, String> kafkaSender) {
        return new ShutdownHookListener(kafkaSender);
    }

    private Properties buildProducerProperties() {

        Properties defaultProducerProperties = ReadPropertiesUtils.readProducerDefaultProperties();
        defaultProducerProperties.setProperty("bootstrap.servers", properties.getBootstrap().getServers());

        String keySerializer = properties.getProducer().getKeySerializer();
        if (Boolean.FALSE.equals(StringUtils.isEmpty(keySerializer))) {
            defaultProducerProperties.setProperty("key.serializer", keySerializer);
        }

        String valueSerializer = properties.getProducer().getValueSerializer();
        if (Boolean.FALSE.equals(StringUtils.isEmpty(valueSerializer))) {
            defaultProducerProperties.setProperty("value.serializer", valueSerializer);
        }
        return defaultProducerProperties;
    }
}
