package com.essexboy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

@Getter
@Setter
@ToString
@NoArgsConstructor
public class Config {

    final static Logger LOGGER = LoggerFactory.getLogger(Config.class);

    private boolean dryrun = true;
    private int delay;
    private Properties kafkaProperties;
    private List<String> topics;

    public static Config getConfig() throws JsonProcessingException {
        if (System.getenv("REBALANCE_CONFIG") == null) {
            throw new RuntimeException("ERROR REBALANCE_CONFIG env is not set");
        }
        final Config config = new ObjectMapper().readValue(System.getenv("REBALANCE_CONFIG"), Config.class);
        Properties properties = new Properties();
        System.getenv().keySet().stream().filter(key -> key.startsWith("KAFKA_")).forEach(key -> {
            String kafkaProperty = key.replace("KAFKA_", "").replace("_", ".").toLowerCase();
            properties.put(kafkaProperty, System.getenv(key));
        });
        config.setKafkaProperties(properties);
        LOGGER.debug("created config {}", config);
        return config;
    }
}

