package com.g7.framework.reactive.kafka.container;

import com.g7.framework.reactive.kafka.comsumer.DefaultMessageComsumer;
import com.g7.framework.reactive.kafka.properties.KafkaProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.SmartLifecycle;
import org.springframework.util.Assert;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
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
    private static final Scheduler SCHEDULER = Schedulers.boundedElastic();
    private final DefaultMessageComsumer comsumer;
    private final String[] topics;
    @Autowired
    private KafkaProperties properties;
    private final String groupId;
    private Disposable subscribe;

    public ReactiveKafkaReceiverContainer(DefaultMessageComsumer comsumer,
                                          String groupId,
                                          String... topics) {
        Assert.noNullElements(topics, "consume topic is not null.");
        this.comsumer = comsumer;
        this.topics = topics;
        this.groupId = groupId;
    }

    @Override
    public void start() {
        final ReceiverOptionsBuilder<String, String> builder = new ReceiverOptionsBuilder<>(properties, topics,
                groupId);
        final ReceiverOptions<String, String> options = builder.build();
        final KafkaReceiver<String, String> receiver = KafkaReceiver.create(options);
        subscribe = receiver.receive()
                .doOnError(throwable ->
                        logger.error("connect kafka failed.", throwable))
                .groupBy(m -> m.receiverOffset().topicPartition())//按分区分组以保证排序
                .flatMap(flux -> flux.publishOn(SCHEDULER)
                        .filter(Objects::nonNull)
                        .mapNotNull(record -> {
                            comsumer.record(record);
                            assert record != null;
                            return record.receiverOffset();
                        })
                        .doOnError(throwable -> logger.error("consume message failed , " +
                                "consumer name is {}", comsumer.getClass().getName(), throwable))
                        .sample(Duration.ofMillis(5000))//定期提交
                        .concatMap(offset -> {
                            if (Objects.nonNull(offset)) {
                                return offset.commit();
                            } else {
                                return Mono.empty();
                            }
                        }))// 使用 concatMap 按顺序提交
                .subscribe();
    }

    @Override
    public void stop() {
        if (Objects.nonNull(subscribe) && Boolean.FALSE.equals(subscribe.isDisposed())) {
            logger.info("Cancel or dispose the underlying task or resource.");
            subscribe.dispose();
        }
    }

    @Override
    public boolean isRunning() {
        return false;
    }
}
