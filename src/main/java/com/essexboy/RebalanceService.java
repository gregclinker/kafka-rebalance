package com.essexboy;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Getter
@ToString
public class RebalanceService {

    final static Logger LOGGER = LoggerFactory.getLogger(RebalanceService.class);

    private final Config config;
    private final ProcessedStats processedStats = new ProcessedStats();
    private final int requiredReplicas = 4;

    public RebalanceService() throws JsonProcessingException {
        this.config = Config.getConfig();
    }

    public void rebalance() {
        LOGGER.debug("running, dryRun={}", config.isDryrun());
        processedStats.reset();
        for (String topic : config.getTopics()) {
            processedStats.topicsRebalanceProcessed++;
            rebalance(topic);
        }
        System.out.println("finished, stats=" + processedStats);
    }

    private void rebalance(String topic) {
        final TopicInfo topicInfo = getTopicData(topic);
        final int partitionMinIsr = getTopicData(topic).getPartitionMinIsr();
        if (partitionMinIsr >= requiredReplicas) {
            processedStats.topicsRebalanceSkipped++;
            LOGGER.info("SKIPPING rebalance topic {}, no ISRs less than {}", topic, requiredReplicas);
        }
        for (PartitionInfo partition : topicInfo.getPartitions()) {
            processedStats.partitionsRebalanceProcessed++;
            try {
                final List<Integer> newReplicas = getRebalanceReplicas(partition.getLeader(), partition.getReplicas());
                if (CollectionUtils.containsAll(partition.getReplicas(), newReplicas)) {
                    LOGGER.info("SKIPPING rebalance topic {}, partition {}, already has 2 replicas on both racks", topic, partition);
                    processedStats.partitionsRebalanceSkipped++;
                } else if (config.isDryrun()) {
                    LOGGER.info("DRYRUN rebalancing topic {}, partition {}, new replicas={}", topic, partition, newReplicas);
                } else {
                    LOGGER.info("rebalancing topic {}, partition {}, existing replicas={}, new replicas={}", topic, partition, partition.getReplicas(), newReplicas);
                    partitionReassignment(topic, partition.getId(), newReplicas);
                    Thread.sleep(config.getDelay() * 1000);
                }
            } catch (Exception e) {
                processedStats.errors++;
                LOGGER.error("ERROR rebalancing topic {}, partition {}", topic, partition.getId(), e);
            }
        }
    }

    public List<Integer> getRebalanceReplicas(int leader, List<Integer> replicas) {
        List<Integer> rack1 = replicas.stream().sorted().filter(replica -> replica <= 3).collect(Collectors.toList());
        List<Integer> rack2 = replicas.stream().sorted().filter(replica -> replica > 3).collect(Collectors.toList());
        if (rack1.size() > 2) {
            final Integer replicaToMove = rack1.stream().filter(r -> r != leader).findFirst().get();
            rack1.remove(replicaToMove);
        } else if (rack2.size() > 2) {
            final Integer replicaToMove = rack2.stream().filter(r -> r != leader).findFirst().get();
            rack2.remove(replicaToMove);
        }
        if (rack1.size() < 2) {
            if (rack1.size() > 0) {
                rack1.add((rack1.get(0) % 3) + 1);
            } else {
                rack1.add(1);
                rack1.add(2);
            }
        }
        if (rack2.size() < 2) {
            if (rack2.size() > 0) {
                rack2.add((rack2.get(0) % 3) + 4);
            } else {
                rack2.add(4);
                rack2.add(5);
            }
        }
        return orderLeaderFirst(leader, Stream.concat(rack1.stream(), rack2.stream()).sorted().collect(Collectors.toList()));
    }

    private void partitionReassignment(String topicName, Integer partition, List<Integer> replicas) throws Exception {
        try (AdminClient adminClient = AdminClient.create(config.getKafkaProperties())) {
            Map<TopicPartition, Optional<NewPartitionReassignment>> topicPartitionOptionalHashMap = new HashMap<>();
            topicPartitionOptionalHashMap.put(new TopicPartition(topicName, partition), Optional.of(new NewPartitionReassignment(replicas)));
            adminClient.alterPartitionReassignments(topicPartitionOptionalHashMap).all().get();
            LOGGER.info("replicas set to {} for topic {}, partition {}", replicas, topicName, partition);
        }
    }

    public TopicInfo getTopicData(String topicName) {
        TopicInfo topicInfo = new TopicInfo();
        topicInfo.setName(topicName);
        try (AdminClient adminClient = AdminClient.create(config.getKafkaProperties())) {
            final TopicDescription topicDescription = adminClient.describeTopics(List.of(topicName)).topicNameValues().get(topicName).get();
            topicInfo.setPartitions(topicDescription.partitions().stream().map(PartitionInfo::new).collect(Collectors.toList()));
        } catch (Exception e) {
            LOGGER.error("ERROR getTopicData, topic {}", topicName, e);
        }
        return topicInfo;
    }

    private List<Integer> orderLeaderFirst(Integer leader, List<Integer> replicas) {
        final List<Integer> newReplicas = replicas.stream().filter(i -> !i.equals(leader)).collect(Collectors.toList());
        newReplicas.add(0, leader);
        return newReplicas;
    }

    private static class ProcessedStats {
        protected int topicsRebalanceProcessed;
        protected int topicsRebalanceSkipped;
        protected int partitionsRebalanceProcessed;
        protected int partitionsRebalanceSkipped;
        protected int errors;
        private long startTime;

        public void reset() {
            topicsRebalanceProcessed = 0;
            topicsRebalanceSkipped = 0;
            partitionsRebalanceProcessed = 0;
            partitionsRebalanceSkipped = 0;
            errors = 0;
            startTime = System.currentTimeMillis();
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("ProcessedStats{");
            if (topicsRebalanceProcessed > 0) {
                sb.append("topicsRebalanceProcessed=").append(topicsRebalanceProcessed);
                sb.append(", topicsRebalanceSkipped=").append(topicsRebalanceSkipped);
            }
            if (partitionsRebalanceProcessed > 0) {
                sb.append(", partitionsRebalanceProcessed=").append(partitionsRebalanceProcessed);
                sb.append(", partitionsRebalanceSkipped=").append(partitionsRebalanceSkipped);
            }
            sb.append(", errors=").append(errors);
            sb.append(", elapsedMinutes=").append(new Interval(startTime, System.currentTimeMillis()).toPeriod().getMinutes());
            sb.append('}');
            return sb.toString();
        }
    }
}