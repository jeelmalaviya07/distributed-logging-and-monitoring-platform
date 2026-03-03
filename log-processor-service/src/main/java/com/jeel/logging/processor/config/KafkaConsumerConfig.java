package com.jeel.logging.processor.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.jeel.logging.common.events.LogIngestedEvent;
import com.jeel.logging.common.events.NormalizedLogEvent;
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
                mapper
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
                mapper
        );
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
            ObjectMapper mapper
    ) {

        Map<String, Object> props = new HashMap<>();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "log-processor-v2");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);

        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);

        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.jeel.logging.common.events");
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
/*
Windows PowerShell
Copyright (C) Microsoft Corporation. All rights reserved.

Install the latest PowerShell for new features and improvements! https://aka.ms/PSWindows

PS C:\Users\hp> docker exec -it redpanda rpk topic consume logs.raw.v1
{
  "topic": "logs.raw.v1",
  "key": "tenant-123",
  "value": "{\"tenantId\":\"tenant-123\",\"requestId\":\"75067511-97f9-4fa8-a338-5a739cea0318\",\"serviceName\":\"user-service\",\"environment\":\"prod\",\"ingestedAt\":1772421766.828603500,\"logs\":[{\"timestamp\":1772385900.000000000,\"level\":\"ERROR\",\"message\":\"NullPointerException at UserService\",\"traceId\":\"trace-123\",\"spanId\":\"span-456\",\"logger\":null,\"thread\":null,\"exception\":null,\"attributes\":null}]}",
  "headers": [
    {
      "key": "__TypeId__",
      "value": "com.jeel.logging.common.events.LogIngestedEvent"
    }
  ],
  "timestamp": 1772421768002,
  "partition": 10,
  "offset": 0
}
{
  "topic": "logs.raw.v1",
  "key": "tenant-123",
  "value": "{\"tenantId\":\"tenant-123\",\"requestId\":\"9965de28-1640-486b-a571-89e2cae45900\",\"serviceName\":\"user-service\",\"environment\":\"prod\",\"ingestedAt\":1772422181.085827100,\"logs\":[{\"timestamp\":1772385900.000000000,\"level\":\"ERROR\",\"message\":\"NullPointerException at UserService\",\"traceId\":\"trace-123\",\"spanId\":\"span-456\",\"logger\":null,\"thread\":null,\"exception\":null,\"attributes\":null}]}",
  "headers": [
    {
      "key": "__TypeId__",
      "value": "com.jeel.logging.common.events.LogIngestedEvent"
    }
  ],
  "timestamp": 1772422181094,
  "partition": 10,
  "offset": 1
}
{
  "topic": "logs.raw.v1",
  "key": "tenant-123",
  "value": "{\"tenantId\":\"tenant-123\",\"requestId\":\"50d14957-8e67-4103-b427-ea73dffcd260\",\"serviceName\":\"user-service\",\"environment\":\"prod\",\"ingestedAt\":1772422204.369625100,\"logs\":[{\"timestamp\":1772385900.000000000,\"level\":\"ERROR\",\"message\":\"NullPointerException at UserService\",\"traceId\":\"trace-123\",\"spanId\":\"span-456\",\"logger\":null,\"thread\":null,\"exception\":null,\"attributes\":null}]}",
  "headers": [
    {
      "key": "__TypeId__",
      "value": "com.jeel.logging.common.events.LogIngestedEvent"
    }
  ],
  "timestamp": 1772422204370,
  "partition": 10,
  "offset": 2
}
{
  "topic": "logs.raw.v1",
  "key": "tenant-1",
  "value": "{\"tenantId\":\"tenant-1\",\"requestId\":\"1596ba04-e1ca-4385-a4a2-ce515787455e\",\"serviceName\":null,\"environment\":\"prod\",\"ingestedAt\":1772422310.386223500,\"logs\":[{\"timestamp\":1772385900.000000000,\"level\":\"ERROR\",\"message\":\"NullPointerException at UserService\",\"traceId\":\"trace-123\",\"spanId\":\"span-456\",\"logger\":null,\"thread\":null,\"exception\":null,\"attributes\":null}]}",
  "headers": [
    {
      "key": "__TypeId__",
      "value": "com.jeel.logging.common.events.LogIngestedEvent"
    }
  ],
  "timestamp": 1772422310387,
  "partition": 11,
  "offset": 0
}
^C
PS C:\Users\hp> docker exec -it redpanda rpk topic consume logs.dlq.v1
^C
PS C:\Users\hp>docker exec -it redpanda rpk topic consume logs.normalized.v1
{
  "topic": "logs.normalized.v1",
  "key": "tenant-123",
  "value": "{\"eventId\":\"a752ed12-7851-4d04-9d06-138eb5a39ae1\",\"tenantId\":\"tenant-123\",\"serviceName\":\"user-service\",\"environment\":\"prod\",\"logLevel\":\"ERROR\",\"message\":\"NullPointerException at UserService\",\"timestamp\":1772385900.000000000,\"exceptionType\":null,\"exceptionMessage\":null,\"fingerprint\":\"3cdfd8f3\",\"traceId\":\"trace-123\",\"spanId\":\"span-456\"}",
  "timestamp": 1772421768558,
  "partition": 10,
  "offset": 0
}
{
  "topic": "logs.normalized.v1",
  "key": "tenant-123",
  "value": "{\"eventId\":\"7b4a8aa2-0c33-4685-8ebe-3a0c9a0e709a\",\"tenantId\":\"tenant-123\",\"serviceName\":\"user-service\",\"environment\":\"prod\",\"logLevel\":\"ERROR\",\"message\":\"NullPointerException at UserService\",\"timestamp\":1772385900.000000000,\"exceptionType\":null,\"exceptionMessage\":null,\"fingerprint\":\"3cdfd8f3\",\"traceId\":\"trace-123\",\"spanId\":\"span-456\"}",
  "timestamp": 1772422166318,
  "partition": 10,
  "offset": 1
}
{
  "topic": "logs.normalized.v1",
  "key": "tenant-123",
  "value": "{\"eventId\":\"1b5bbf5c-75bd-4c3d-b34d-77a49e1d3f62\",\"tenantId\":\"tenant-123\",\"serviceName\":\"user-service\",\"environment\":\"prod\",\"logLevel\":\"ERROR\",\"message\":\"NullPointerException at UserService\",\"timestamp\":1772385900.000000000,\"exceptionType\":null,\"exceptionMessage\":null,\"fingerprint\":\"3cdfd8f3\",\"traceId\":\"trace-123\",\"spanId\":\"span-456\"}",
  "timestamp": 1772422181121,
  "partition": 10,
  "offset": 2
}
{
  "topic": "logs.normalized.v1",
  "key": "tenant-123",
  "value": "{\"eventId\":\"eaf6c6b1-4de0-45e2-bc49-5ec5f081b394\",\"tenantId\":\"tenant-123\",\"serviceName\":\"user-service\",\"environment\":\"prod\",\"logLevel\":\"ERROR\",\"message\":\"NullPointerException at UserService\",\"timestamp\":1772385900.000000000,\"exceptionType\":null,\"exceptionMessage\":null,\"fingerprint\":\"3cdfd8f3\",\"traceId\":\"trace-123\",\"spanId\":\"span-456\"}",
  "timestamp": 1772422204425,
  "partition": 10,
  "offset": 3
}
{
  "topic": "logs.normalized.v1",
  "key": "tenant-1",
  "value": "{\"eventId\":\"6b0023eb-64ec-4478-9c17-102a2a9c810b\",\"tenantId\":\"tenant-1\",\"serviceName\":null,\"environment\":\"prod\",\"logLevel\":\"ERROR\",\"message\":\"NullPointerException at UserService\",\"timestamp\":1772385900.000000000,\"exceptionType\":null,\"exceptionMessage\":null,\"fingerprint\":\"cd258e5e\",\"traceId\":\"trace-123\",\"spanId\":\"span-456\"}",
  "timestamp": 1772422310432,
  "partition": 11,
  "offset": 0
}
^C
PS C:\Users\hp> docker ps
CONTAINER ID   IMAGE                            COMMAND                  CREATED        STATUS       PORTS
   NAMES
c75f9c211aa5   provectuslabs/kafka-ui:latest    "/bin/sh -c 'java --…"   33 hours ago   Up 8 hours   0.0.0.0:8081->8080/tcp, [::]:8081->8080/tcp   kafka-ui
3827dd42865f   redis:alpine                     "docker-entrypoint.s…"   33 hours ago   Up 8 hours   0.0.0.0:6379->6379/tcp, [::]:6379->6379/tcp   redis
f342dbdcc21a   redpandadata/redpanda:v23.2.15   "/entrypoint.sh redp…"   33 hours ago   Up 8 hours   0.0.0.0:9092->9092/tcp, [::]:9092->9092/tcp   redpanda
1b7c1f7cfd7d   postgres:15                      "docker-entrypoint.s…"   33 hours ago   Up 8 hours   0.0.0.0:5432->5432/tcp, [::]:5432->5432/tcp   postgres
PS C:\Users\hp> docker exec -it postgres psql -U jeel -d log_intelligence
psql (15.17 (Debian 15.17-1.pgdg13+1))
Type "help" for help.

log_intelligence=# SELECT id, tenant_id, service_name, environment, fingerprint, occurrence_count
FROM error_groups;
                  id                  | tenant_id  | service_name | environment | fingerprint | occurrence_count
--------------------------------------+------------+--------------+-------------+-------------+------------------
 d7343700-5fef-4b4b-9680-29c96bd13a7a | tenant-123 | user-service | prod        | 3cdfd8f3    |                4
(1 row)

log_intelligence=#
SELECT id, tenant_id, service_name, message, log_level
FROM error_occurrences;
                  id                  | tenant_id  | service_name |               message               | log_level
--------------------------------------+------------+--------------+-------------------------------------+-----------
 5385c619-2c5c-4a8d-b0ef-a74273c5cdfa | tenant-123 | user-service | NullPointerException at UserService | ERROR
 8a51c7db-f128-4707-a152-2502c8cbce08 | tenant-123 | user-service | NullPointerException at UserService | ERROR
 2ff6c124-12ac-4f4d-8ffa-26667e697c0f | tenant-123 | user-service | NullPointerException at UserService | ERROR
(3 rows)

log_intelligence=#
SELECT * FROM processed_event_entity;
ERROR:  relation "processed_event_entity" does not exist
LINE 1: SELECT * FROM processed_event_entity;
                      ^
log_intelligence=#\t
Tuples only is on.
log_intelligence=#\dt
 public | alert_history     | table | jeel
 public | alert_rules       | table | jeel
 public | dlq_events        | table | jeel
 public | error_groups      | table | jeel
 public | error_occurrences | table | jeel
 public | processed_events  | table | jeel

log_intelligence=#
SELECT * FROM processed_events;
 tenant-123:7b4a8aa2-0c33-4685-8ebe-3a0c9a0e709a | 2026-03-02 03:29:26.625359+00
 tenant-123:1b5bbf5c-75bd-4c3d-b34d-77a49e1d3f62 | 2026-03-02 03:29:41.17648+00
 tenant-123:eaf6c6b1-4de0-45e2-bc49-5ec5f081b394 | 2026-03-02 03:30:04.477754+00

log_intelligence=#
SELECT id, tenant_id, service_name, message, log_level
FROM error_occurrences;
 5385c619-2c5c-4a8d-b0ef-a74273c5cdfa | tenant-123 | user-service | NullPointerException at UserService | ERROR
 8a51c7db-f128-4707-a152-2502c8cbce08 | tenant-123 | user-service | NullPointerException at UserService | ERROR
 2ff6c124-12ac-4f4d-8ffa-26667e697c0f | tenant-123 | user-service | NullPointerException at UserService | ERROR

log_intelligence=#\dt
 public | alert_history     | table | jeel
 public | alert_rules       | table | jeel
 public | dlq_events        | table | jeel
 public | error_groups      | table | jeel
 public | error_occurrences | table | jeel
 public | processed_events  | table | jeel

log_intelligence=# SELECT * FROM alert_history
log_intelligence-# ;

log_intelligence=# SELECT * FROM dlq_events;

log_intelligence=# SELECT * FROM error_groups;
log_intelligence=#
log_intelligence=# SELECT * FROM error_occurrences;
log_intelligence=#SELECT * FROM processed_events;
                   event_id                    |         processed_at
-----------------------------------------------+------------------------------
 tenant-1:fd7c7f81-29fe-48ec-a463-fe64e36b4cd5 | 2026-03-02 04:27:43.93969+00
(1 row)

log_intelligence=#docker exec -it redpanda rpk topic consume logs.dlq.v1
log_intelligence-#SELECT * FROM error_occurrences;
PS C:\Users\hp>docker exec -it redpanda rpk topic consume logs.dlq.v1
{
  "topic": "logs.dlq.v1",
  "key": "tenant-1",
  "value": "{\"eventId\":\"888da9f1-945a-4aeb-8fa6-bc630d49d21e\",\"tenantId\":\"tenant-1\",\"serviceName\":null,\"environment\":\"prod\",\"logLevel\":\"ERROR\",\"message\":\"NullPointerException at UserService\",\"timestamp\":1772385900.000000000,\"exceptionType\":null,\"exceptionMessage\":null,\"fingerprint\":\"cd258e5e\",\"traceId\":\"trace-123\",\"spanId\":\"span-456\"}",
  "headers": [
    {
      "key": "x-failure-reason",
      "value": "NON_RETRYABLE: not-null property references a null or transient value : com.jeel.logging.processor.persistence.entity.ErrorGroupEntity.serviceName"
    },
    {
      "key": "x-final-retry-count",
      "value": "0"
    }
  ],
  "timestamp": 1772423935866,
  "partition": 0,
  "offset": 0
}
{
  "topic": "logs.dlq.v1",
  "key": "tenant-1",
  "value": "{\"eventId\":\"73eb47ea-1317-411e-94c8-eefa94cb4fa2\",\"tenantId\":\"tenant-1\",\"serviceName\":null,\"environment\":\"prod\",\"logLevel\":\"ERROR\",\"message\":\"NullPointerException at UserService\",\"timestamp\":1772385900.000000000,\"exceptionType\":null,\"exceptionMessage\":null,\"fingerprint\":\"cd258e5e\",\"traceId\":\"trace-123\",\"spanId\":\"span-456\"}",
  "headers": [
    {
      "key": "x-failure-reason",
      "value": "NON_RETRYABLE: not-null property references a null or transient value : com.jeel.logging.processor.persistence.entity.ErrorGroupEntity.serviceName"
    },
    {
      "key": "x-final-retry-count",
      "value": "0"
    }
  ],
  "timestamp": 1772423959025,
  "partition": 0,
  "offset": 1
}
{
  "topic": "logs.dlq.v1",
  "key": "tenant-1",
  "value": "{\"eventId\":\"ba0f19af-f774-4b0d-ba73-80cf00e7d99b\",\"tenantId\":\"tenant-1\",\"serviceName\":null,\"environment\":\"prod\",\"logLevel\":\"ERROR\",\"message\":\"NullPointerException at UserService\",\"timestamp\":1772385900.000000000,\"exceptionType\":null,\"exceptionMessage\":null,\"fingerprint\":\"cd258e5e\",\"traceId\":\"trace-123\",\"spanId\":\"span-456\"}",
  "headers": [
    {
      "key": "x-failure-reason",
      "value": "NON_RETRYABLE: not-null property references a null or transient value : com.jeel.logging.processor.persistence.entity.ErrorGroupEntity.serviceName"
    },
    {
      "key": "x-final-retry-count",
      "value": "0"
    }
  ],
  "timestamp": 1772424053102,
  "partition": 0,
  "offset": 2
}
{
  "topic": "logs.dlq.v1",
  "key": "tenant-1",
  "value": "{\"eventId\":\"7b2a5316-5117-4d00-9170-7b130669bdf8\",\"tenantId\":\"tenant-1\",\"serviceName\":null,\"environment\":\"prod\",\"logLevel\":\"ERROR\",\"message\":\"NullPointerException at UserService\",\"timestamp\":1772385900.000000000,\"exceptionType\":null,\"exceptionMessage\":null,\"fingerprint\":\"cd258e5e\",\"traceId\":\"trace-123\",\"spanId\":\"span-456\"}",
  "headers": [
    {
      "key": "x-failure-reason",
      "value": "NON_RETRYABLE: not-null property references a null or transient value : com.jeel.logging.processor.persistence.entity.ErrorGroupEntity.serviceName"
    },
    {
      "key": "x-final-retry-count",
      "value": "0"
    }
  ],
  "timestamp": 1772424113410,
  "partition": 0,
  "offset": 3
}
{
  "topic": "logs.dlq.v1",
  "key": "tenant-1",
  "value": "{\"eventId\":\"bb359d98-7662-43ec-a359-0955d6089cc4\",\"tenantId\":\"tenant-1\",\"serviceName\":null,\"environment\":\"prod\",\"logLevel\":\"ERROR\",\"message\":\"NullPointerException occurred\",\"timestamp\":1772445600.000000000,\"exceptionType\":\"NullPointerException\",\"exceptionMessage\":\"Cannot invoke method on null\",\"fingerprint\":\"be41d099\",\"traceId\":\"abc123\",\"spanId\":\"def456\"}",
  "headers": [
    {
      "key": "x-failure-reason",
      "value": "NON_RETRYABLE: not-null property references a null or transient value : com.jeel.logging.processor.persistence.entity.ErrorGroupEntity.serviceName"
    },
    {
      "key": "x-final-retry-count",
      "value": "0"
    }
  ],
  "timestamp": 1772426353023,
  "partition": 0,
  "offset": 4
}

 */
