package com.jeel.logging.processor.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.jeel.logging.common.events.LogIngestedEvent;
import com.jeel.logging.common.events.NormalizedLogEvent;
import com.jeel.logging.processor.alert.AlertEvaluateEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    @Bean
    public ObjectMapper kafkaObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        return mapper;
    }

    // 🔵 RAW EVENT CONSUMER
    @Bean
    public ConsumerFactory<String, LogIngestedEvent> rawConsumerFactory(ObjectMapper mapper) {

        return buildConsumerFactory(
                LogIngestedEvent.class,
                mapper,
                "raw-consumer"
        );
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, LogIngestedEvent>
    rawKafkaListenerContainerFactory(
            ConsumerFactory<String, LogIngestedEvent> rawConsumerFactory) {

        ConcurrentKafkaListenerContainerFactory<String, LogIngestedEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(rawConsumerFactory);
        factory.getContainerProperties().setAckMode(
                org.springframework.kafka.listener.ContainerProperties.AckMode.MANUAL
        );

        return factory;
    }

    // 🔵 NORMALIZED EVENT CONSUMER
    @Bean
    public ConsumerFactory<String, NormalizedLogEvent> normalizedConsumerFactory(ObjectMapper mapper) {

        return buildConsumerFactory(
                NormalizedLogEvent.class,
                mapper,
                "normalized-consumer"
        );
    }

    @Bean
    public ConsumerFactory<String, AlertEvaluateEvent> alertConsumerFactory(ObjectMapper mapper) {

        return buildConsumerFactory(
                AlertEvaluateEvent.class,
                mapper,
                "alert-worker"
        );
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, AlertEvaluateEvent>
    alertKafkaListenerContainerFactory(
            ConsumerFactory<String, AlertEvaluateEvent> alertConsumerFactory) {

        ConcurrentKafkaListenerContainerFactory<String, AlertEvaluateEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(alertConsumerFactory);

        factory.getContainerProperties().setAckMode(
                org.springframework.kafka.listener.ContainerProperties.AckMode.MANUAL
        );

        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, NormalizedLogEvent>
    normalizedKafkaListenerContainerFactory(
            ConsumerFactory<String, NormalizedLogEvent> normalizedConsumerFactory) {

        ConcurrentKafkaListenerContainerFactory<String, NormalizedLogEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(normalizedConsumerFactory);
        factory.getContainerProperties().setAckMode(
                org.springframework.kafka.listener.ContainerProperties.AckMode.MANUAL
        );

        return factory;
    }

    // 🔧 COMMON FACTORY BUILDER
    private <T> ConsumerFactory<String, T> buildConsumerFactory(
            Class<T> clazz,
            ObjectMapper mapper,
            String groupId
    ) {

        Map<String, Object> props = new HashMap<>();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);

        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);

        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);

        JsonDeserializer<T> jsonDeserializer =
                new JsonDeserializer<>(clazz, mapper, false);

        return new DefaultKafkaConsumerFactory<>(
                props,
                new StringDeserializer(),
                new ErrorHandlingDeserializer<>(jsonDeserializer)
        );
    }
}
