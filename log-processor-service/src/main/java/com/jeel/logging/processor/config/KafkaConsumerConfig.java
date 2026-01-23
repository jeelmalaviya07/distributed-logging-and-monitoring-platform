package com.jeel.logging.processor.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.jeel.logging.common.events.LogIngestedEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    // 🔥 ObjectMapper that understands Instant, LocalDateTime, etc.
    @Bean
    public ObjectMapper kafkaObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        return mapper;
    }

    @Bean
    public ConsumerFactory<String, LogIngestedEvent> consumerFactory(ObjectMapper kafkaObjectMapper) {

        Map<String, Object> props = new HashMap<>();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "log-processor-v1");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        // 🔥 Error-handling deserializers
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);

        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);

        // Trust your package
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.jeel.logging.common.events");

        // Tell default target type
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE,
                "com.jeel.logging.common.events.LogIngestedEvent");

        // Disable type headers (we send plain JSON)
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);

        // 🔥 Custom JsonDeserializer with ObjectMapper that supports Instant
        JsonDeserializer<LogIngestedEvent> jsonDeserializer =
                new JsonDeserializer<>(LogIngestedEvent.class, kafkaObjectMapper, false);

        return new DefaultKafkaConsumerFactory<>(
                props,
                new StringDeserializer(),
                new ErrorHandlingDeserializer<>(jsonDeserializer)
        );
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, LogIngestedEvent>
    kafkaListenerContainerFactory(ConsumerFactory<String, LogIngestedEvent> consumerFactory) {

        ConcurrentKafkaListenerContainerFactory<String, LogIngestedEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory);

        // Manual ack (future safe for retries / DLQ)
        factory.getContainerProperties().setAckMode(
                org.springframework.kafka.listener.ContainerProperties.AckMode.MANUAL
        );

        return factory;
    }
}
