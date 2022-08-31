package com.essexboy;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class ProtoTest {

    @Test
    @SetEnvironmentVariable(key = "KAFKA_BOOTSTRAP_SERVERS", value = "172.19.0.6:9092,172.19.0.9:9093,172.19.0.10:9094,172.19.0.7:9095,172.19.0.5:9096,172.19.0.8:9097")
    @SetEnvironmentVariable(key = "KAFKA_SECURITY_PROTOCOL", value = "SSL")
    @SetEnvironmentVariable(key = "KAFKA_SSL_TRUSTSTORE_LOCATION", value = "/home/greg/work/kafka-heartbeat/secrets/kafka_truststore.jks")
    @SetEnvironmentVariable(key = "KAFKA_SSL_TRUSTSTORE_PASSWORD", value = "confluent")
    @SetEnvironmentVariable(key = "KAFKA_SSL_KEYSTORE_LOCATION", value = "/home/greg/work/kafka-heartbeat/secrets/kafka_keystore.jks")
    @SetEnvironmentVariable(key = "KAFKA_SSL_KEYSTORE_PASSWORD", value = "confluent")
    @SetEnvironmentVariable(key = "KAFKA_SSL_KEY_PASSWORD", value = "confluent")
    @SetEnvironmentVariable(key = "KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM", value = " ")
    @SetEnvironmentVariable(key = "HEART_BEAT_CONFIG", value = "{\"numberOfBrokers\":3,\"interval\":10,\"standardIsr\":2,\"reducedIsr\":1,\"countToSwitch\":3,\"topics\":[\"greg-test1\",\"greg-test2\"]}")
    public void test2() throws Exception {
    }
}
