package com.g7.framework.reactive.kafka;

import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

/**
 * @author dreamyao
 * @title
 * @date 2019-04-16 22:19
 * @since 1.0.0
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
@Import({ReactiveKafkaAutoConfiguration.class})
public @interface EnableReactiveKafka {
}
