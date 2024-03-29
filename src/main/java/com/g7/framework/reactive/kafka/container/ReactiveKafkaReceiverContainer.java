package com.g7.framework.reactive.kafka.container;

import com.g7.framework.reactive.kafka.comsumer.AbstractMessageComsumer;
import com.g7.framework.reactive.kafka.properties.KafkaProperties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.sleuth.Span;
import org.springframework.cloud.sleuth.propagation.Propagator;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.core.ResolvableType;
import org.springframework.util.Assert;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.time.Duration;
import java.util.Objects;

/**
 * @author dreamyao
 * @title
 * @date 2022/1/27 10:56 下午
 * @since 1.0.0
 */
public class ReactiveKafkaReceiverContainer implements SmartLifecycle {

    private static final Logger logger = LoggerFactory.getLogger(ReactiveKafkaReceiverContainer.class);
    private static final String TRACE_ID = "traceId";
    private static final String SPAN_ID = "spanId";
    private final AbstractMessageComsumer comsumer;
    private final String[] topics;
    @Autowired
    private KafkaProperties properties;
    @Autowired
    private BeanFactory beanFactory;
    private final String groupId;
    private Disposable subscribe;
    private Propagator propagator;
    private Propagator.Getter<ConsumerRecord<?, ?>> extractor;

    public ReactiveKafkaReceiverContainer(AbstractMessageComsumer comsumer,
                                          String groupId,
                                          String... topics) {
        Assert.noNullElements(topics, "consume topic is not null.");
        this.comsumer = comsumer;
        this.topics = topics;
        this.groupId = groupId;
    }

    @Override
    public void start() {
        final ReceiverOptionsBuilder<String, String> builder = new ReceiverOptionsBuilder<>(properties,
                topics, groupId);
        final ReceiverOptions<String, String> options = builder.build();
        final KafkaReceiver<String, String> receiver = KafkaReceiver.create(options);
        subscribe = receiver.receive()
                .doOnError(throwable ->
                        logger.error("connect kafka failed.", throwable))
                .groupBy(m -> m.receiverOffset().topicPartition())//按分区分组以保证排序
                .flatMap(flux -> flux.publishOn(Schedulers.boundedElastic())
                        .filter(Objects::nonNull)
                        .doOnNext(record -> buildAndFinishSpan(record, propagator(),
                                extractor()))
                        .flatMap(record -> comsumer.consume(record)
                                .map(obj -> record.receiverOffset()))
                        .doOnError(throwable -> logger.error("consume message failed , " +
                                "consumer name is {}", comsumer.getClass().getName(), throwable))
                        .sample(Duration.ofMillis(1000))//定期提交
                        .concatMap(offset -> {
                            if (Objects.nonNull(offset)) {
                                return offset.commit();
                            } else {
                                return Mono.empty();
                            }
                        }))// 使用 concatMap 按顺序提交
                .subscribe();
        if (logger.isDebugEnabled()) {
            logger.debug("start reactive kafka consumer container monitor success topics is {} group is {}", topics,
                    groupId);
        }
    }

    @Override
    public void stop() {
        if (Objects.nonNull(subscribe) && Boolean.FALSE.equals(subscribe.isDisposed())) {
            if (logger.isDebugEnabled()) {
                logger.debug("Cancel or dispose the underlying task or resource.");
            }
            subscribe.dispose();
        }
    }

    @Override
    public boolean isRunning() {
        return false;
    }

    private Propagator propagator() {
        if (this.propagator == null) {
            this.propagator = this.beanFactory.getBean(Propagator.class);
        }
        return this.propagator;
    }

    @SuppressWarnings("unchecked")
    private Propagator.Getter<ConsumerRecord<?, ?>> extractor() {
        if (this.extractor == null) {
            this.extractor = (Propagator.Getter<ConsumerRecord<?, ?>>) beanFactory
                    .getBeanProvider(ResolvableType.forClassWithGenerics(Propagator.Getter.class,
                            ResolvableType.forType(
                                    new ParameterizedTypeReference<ConsumerRecord<?, ?>>() {
                            })))
                    .getIfAvailable();
        }
        return this.extractor;
    }

    private <K, V> void buildAndFinishSpan(ConsumerRecord<K, V> consumerRecord,
                                           Propagator propagator,
                                           Propagator.Getter<ConsumerRecord<?, ?>> extractor) {
        Span span = propagator
                .extract(consumerRecord, extractor)
                .kind(Span.Kind.CONSUMER)
                .name("kafka.consume")
                .tag("kafka.topic", consumerRecord.topic())
                .tag("kafka.offset", Long.toString(consumerRecord.offset()))
                .tag("kafka.partition", Integer.toString(consumerRecord.partition()))
                .start();
        if (logger.isDebugEnabled()) {
            logger.debug("Extracted span from event headers " + span);
        }

        MDC.put(TRACE_ID, span.context().traceId());
        MDC.put(SPAN_ID, span.context().spanId());

        span.end();
    }
}
