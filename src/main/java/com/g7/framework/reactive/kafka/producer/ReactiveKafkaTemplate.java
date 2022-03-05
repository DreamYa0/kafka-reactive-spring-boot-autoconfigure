package com.g7.framework.reactive.kafka.producer;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.DisposableBean;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * @author dreamyao
 * @title
 * @date 2022/3/5 3:41 下午
 * @since 1.0.0
 */
public class ReactiveKafkaTemplate implements AutoCloseable, DisposableBean {

    private final KafkaSender<String, String> sender;

    public ReactiveKafkaTemplate(KafkaSender<String, String> sender) {
        this.sender = sender;
    }

    public Mono<SenderResult<Void>> send(String topic, String value) {
        return send(new ProducerRecord<>(topic, value));
    }

    public Mono<SenderResult<Void>> send(String topic, String key, String value) {
        return send(new ProducerRecord<>(topic, key, value));
    }

    public Mono<SenderResult<Void>> send(String topic, int partition, String key, String value) {
        return send(new ProducerRecord<>(topic, partition, key, value));
    }

    public Mono<SenderResult<Void>> send(String topic, int partition, long timestamp, String key, String value) {
        return send(new ProducerRecord<>(topic, partition, timestamp, key, value));
    }

    public Mono<SenderResult<Void>> send(ProducerRecord<String, String> record) {
        return send(SenderRecord.create(record, null));
    }

    public <T> Mono<SenderResult<T>> send(SenderRecord<String, String, T> record) {
        return send(Mono.just(record)).single();
    }

    public <T> Flux<SenderResult<T>> send(Publisher<? extends SenderRecord<String, String, T>> records) {
        return this.sender.send(records);
    }

    public Flux<PartitionInfo> partitionsFromProducerFor(String topic) {
        Mono<List<PartitionInfo>> partitionsInfo = doOnProducer(producer -> producer.partitionsFor(topic));
        return partitionsInfo.flatMapIterable(Function.identity());
    }

    public Flux<Tuple2<MetricName, ? extends Metric>> metricsFromProducer() {
        return doOnProducer(Producer::metrics)
                .flatMapIterable(Map::entrySet)
                .map(m -> Tuples.of(m.getKey(), m.getValue()));
    }

    public <T> Mono<T> doOnProducer(Function<Producer<String, String>, ? extends T> action) {
        return this.sender.doOnProducer(action);
    }

    @Override
    public void destroy() {
        doClose();
    }

    @Override
    public void close() {
        doClose();
    }

    private void doClose() {
        this.sender.close();
    }
}
