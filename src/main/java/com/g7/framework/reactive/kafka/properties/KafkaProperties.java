package com.g7.framework.reactive.kafka.properties;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

/**
 * @author dreamyao
 * @title
 * @date 2019-04-20 17:42
 * @since 1.0.0
 */
@ConfigurationProperties(prefix = "kafka")
@ConditionalOnProperty(prefix = "kafka.bootstrap.servers")
public class KafkaProperties {

    @NestedConfigurationProperty
    private Bootstrap bootstrap = new Bootstrap();
    @NestedConfigurationProperty
    private ConsumerProperties consumer = new ConsumerProperties();
    @NestedConfigurationProperty
    private ProducerProperties producer = new ProducerProperties();

    public Bootstrap getBootstrap() {
        return bootstrap;
    }

    public void setBootstrap(Bootstrap bootstrap) {
        this.bootstrap = bootstrap;
    }

    public ConsumerProperties getConsumer() {
        return consumer;
    }

    public void setConsumer(ConsumerProperties consumer) {
        this.consumer = consumer;
    }

    public ProducerProperties getProducer() {
        return producer;
    }

    public void setProducer(ProducerProperties producer) {
        this.producer = producer;
    }


    public static class Bootstrap{

        private String servers;

        public String getServers() {
            return servers;
        }

        public void setServers(String servers) {
            this.servers = servers;
        }
    }

    public static class ConsumerProperties{

        private String keyDeserializer = "";
        private String valueDeserializer = "";

        public String getKeyDeserializer() {
            return keyDeserializer;
        }

        public void setKeyDeserializer(String keyDeserializer) {
            this.keyDeserializer = keyDeserializer;
        }

        public String getValueDeserializer() {
            return valueDeserializer;
        }

        public void setValueDeserializer(String valueDeserializer) {
            this.valueDeserializer = valueDeserializer;
        }
    }

    public static class ProducerProperties{

        private String keySerializer = "";
        private String valueSerializer = "";

        public String getKeySerializer() {
            return keySerializer;
        }

        public void setKeySerializer(String keySerializer) {
            this.keySerializer = keySerializer;
        }

        public String getValueSerializer() {
            return valueSerializer;
        }

        public void setValueSerializer(String valueSerializer) {
            this.valueSerializer = valueSerializer;
        }
    }


}
