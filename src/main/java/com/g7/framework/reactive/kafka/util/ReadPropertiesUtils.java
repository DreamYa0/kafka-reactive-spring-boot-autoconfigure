package com.g7.framework.reactive.kafka.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author dreamyao
 * @title
 * @date 2018/8/4 下午9:37
 * @since 1.0.0
 */
public class ReadPropertiesUtils {

    private static final Logger logger = LoggerFactory.getLogger(ReadPropertiesUtils.class);

    public static Properties readProducerDefaultProperties() {
        Properties properties = new Properties();
        try {
            InputStream producerPropertiesStream = ReadPropertiesUtils.class.getResourceAsStream(
                    "/producer.properties");

            properties.load(producerPropertiesStream);

        } catch (IOException e) {
            logger.error("read producer properties failed.", e);
        }
        return properties;
    }

    public static Properties readConsumerDefaultProperties() {
        Properties properties = new Properties();
        try {
            InputStream consumerPropertiesStream = ReadPropertiesUtils.class.getResourceAsStream(
                    "/consumer.properties");
            properties.load(consumerPropertiesStream);
        } catch (IOException e) {
            logger.error("read consumer properties failed.", e);
        }
        return properties;
    }
}
