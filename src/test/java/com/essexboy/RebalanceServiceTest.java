package com.essexboy;

import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class RebalanceServiceTest {

    @Test
    @SetEnvironmentVariable(key = "KAFKA_BOOTSTRAP_SERVERS", value = "172.31.0.6:29092,172.31.0.5:29093,172.31.0.7:29094")
    @SetEnvironmentVariable(key = "KAFKA_SECURITY_PROTOCOL", value = "SSL")
    @SetEnvironmentVariable(key = "KAFKA_SSL_TRUSTSTORE_LOCATION", value = "/home/greg/work/kafka-heartbeat/secrets/kafka_truststore.jks")
    @SetEnvironmentVariable(key = "KAFKA_SSL_TRUSTSTORE_PASSWORD", value = "confluent")
    @SetEnvironmentVariable(key = "KAFKA_SSL_KEYSTORE_LOCATION", value = "/home/greg/work/kafka-heartbeat/secrets/kafka_keystore.jks")
    @SetEnvironmentVariable(key = "KAFKA_SSL_KEYSTORE_PASSWORD", value = "confluent")
    @SetEnvironmentVariable(key = "KAFKA_SSL_KEY_PASSWORD", value = "confluent")
    @SetEnvironmentVariable(key = "KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM", value = " ")
    @SetEnvironmentVariable(key = "REBALANCE_CONFIG", value = "{\"delay\":30,\"topics\":[\"greg-test1\",\"greg-test2\"]}")
    public void test1() throws Exception {
        final InputStream inputStream = getClass().getResourceAsStream("/rebalance-config.json");
        assertNotNull(inputStream);
        final RebalanceService rebalanceService = new RebalanceService();

        // do nothing
        assertEquals(Arrays.asList(1, 2, 4, 5), rebalanceService.getRebalanceReplicas(1, Arrays.asList(1, 2, 4, 5)));
        assertEquals(Arrays.asList(2, 1, 4, 5), rebalanceService.getRebalanceReplicas(2, Arrays.asList(1, 2, 4, 5)));
        assertEquals(Arrays.asList(3, 1, 4, 5), rebalanceService.getRebalanceReplicas(3, Arrays.asList(1, 3, 4, 5)));
        assertEquals(Arrays.asList(4, 1, 2, 5), rebalanceService.getRebalanceReplicas(4, Arrays.asList(1, 2, 4, 5)));
        assertEquals(Arrays.asList(5, 1, 2, 4), rebalanceService.getRebalanceReplicas(5, Arrays.asList(1, 2, 4, 5)));
        assertEquals(Arrays.asList(6, 1, 3, 4), rebalanceService.getRebalanceReplicas(6, Arrays.asList(1, 3, 4, 6)));

        // too many on rack 1
        assertEquals(Arrays.asList(1, 3, 4, 5), rebalanceService.getRebalanceReplicas(1, Arrays.asList(1, 2, 3, 4)));
        assertEquals(Arrays.asList(1, 3, 5, 6), rebalanceService.getRebalanceReplicas(1, Arrays.asList(1, 2, 3, 5)));
        assertEquals(Arrays.asList(1, 3, 4, 6), rebalanceService.getRebalanceReplicas(1, Arrays.asList(1, 2, 3, 6)));
        assertEquals(Arrays.asList(2, 3, 4, 5), rebalanceService.getRebalanceReplicas(2, Arrays.asList(1, 2, 3, 4)));
        assertEquals(Arrays.asList(2, 3, 5, 6), rebalanceService.getRebalanceReplicas(2, Arrays.asList(1, 2, 3, 5)));
        assertEquals(Arrays.asList(2, 3, 4, 6), rebalanceService.getRebalanceReplicas(2, Arrays.asList(1, 2, 3, 6)));
        assertEquals(Arrays.asList(3, 2, 4, 5), rebalanceService.getRebalanceReplicas(3, Arrays.asList(1, 2, 3, 4)));
        assertEquals(Arrays.asList(3, 2, 5, 6), rebalanceService.getRebalanceReplicas(3, Arrays.asList(1, 2, 3, 5)));
        assertEquals(Arrays.asList(3, 2, 4, 6), rebalanceService.getRebalanceReplicas(3, Arrays.asList(1, 2, 3, 6)));

        // too many on rack 2
        assertEquals(Arrays.asList(1, 2, 5, 6), rebalanceService.getRebalanceReplicas(1, Arrays.asList(1, 4, 5, 6)));
        assertEquals(Arrays.asList(2, 3, 5, 6), rebalanceService.getRebalanceReplicas(2, Arrays.asList(2, 4, 5, 6)));
        assertEquals(Arrays.asList(3, 1, 5, 6), rebalanceService.getRebalanceReplicas(3, Arrays.asList(3, 4, 5, 6)));
        assertEquals(Arrays.asList(4, 1, 2, 6), rebalanceService.getRebalanceReplicas(4, Arrays.asList(1, 4, 5, 6)));
        assertEquals(Arrays.asList(5, 2, 3, 6), rebalanceService.getRebalanceReplicas(5, Arrays.asList(2, 4, 5, 6)));
        assertEquals(Arrays.asList(6, 1, 3, 5), rebalanceService.getRebalanceReplicas(6, Arrays.asList(3, 4, 5, 6)));

        // under replicated
        assertEquals(Arrays.asList(1, 2, 4, 5), rebalanceService.getRebalanceReplicas(1, Arrays.asList(1, 4)));
        assertEquals(Arrays.asList(1, 2, 4, 5), rebalanceService.getRebalanceReplicas(1, Arrays.asList(1, 2, 4)));
        assertEquals(Arrays.asList(1, 2, 4, 5), rebalanceService.getRebalanceReplicas(1, Arrays.asList(1, 4, 5)));
        assertEquals(Arrays.asList(1, 3, 4, 5), rebalanceService.getRebalanceReplicas(1, Arrays.asList(1, 2, 3)));
        assertEquals(Arrays.asList(4, 1, 2, 6), rebalanceService.getRebalanceReplicas(4, Arrays.asList(4, 5, 6)));
        assertEquals(Arrays.asList(5, 1, 2, 6), rebalanceService.getRebalanceReplicas(5, Arrays.asList(4, 5, 6)));
    }
}