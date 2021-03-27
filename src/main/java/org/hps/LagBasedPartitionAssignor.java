package org.hps;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;


public class LagBasedPartitionAssignor implements ConsumerPartitionAssignor, Configurable {

    private static final Logger LOGGER = LoggerFactory.getLogger(LagBasedPartitionAssignor.class);

    private Properties consumerGroupProps;
    private Properties metadataConsumerProps;
    private KafkaConsumer<byte[], byte[]> metadataConsumer;

   // this will create a metadata consumer that would not particpate in the consumption
    //and that is needed to to get offset and metada...
    @Override
    public void configure(Map<String, ?> configs) {

        // Construct Properties from config map
        consumerGroupProps = new Properties();
        for (final Map.Entry<String, ?> prop : configs.entrySet()) {
            consumerGroupProps.put(prop.getKey(), prop.getValue());
        }

        // group.id must be defined
        final String groupId = consumerGroupProps.getProperty(ConsumerConfig.GROUP_ID_CONFIG);
        if (groupId == null) {
            throw new IllegalArgumentException(
                    ConsumerConfig.GROUP_ID_CONFIG + " cannot be null when using "
                            + ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG + "="
                            + this.getClass().getName());
        }

        // Create a new consumer that can be used to get lag metadata for the consumer group
        metadataConsumerProps = new Properties();
        metadataConsumerProps.putAll(consumerGroupProps);
        metadataConsumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        final String clientId = groupId + ".assignor";
        metadataConsumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);

        LOGGER.info(
                "Configured LagBasedPartitionAssignor with values:\n"
                        + "\tgroup.id = {}\n"
                        + "\tclient.id = {}\n",
                groupId,
                clientId
        );

    }


    @Override
    public ByteBuffer subscriptionUserData(Set<String> topics) {

        // TODO
        // if there is something (custom data) that need to be sent
        // along with the subscription
        return null;
    }

    @Override
    public void onAssignment(Assignment assignment, ConsumerGroupMetadata metadata) {
        // TODO
        // if there is something to that is returned and to be saved across generations
    }

    @Override
    public String name() {
        return "lag";
    }

    @Override
    public GroupAssignment assign(Cluster metadata, GroupSubscription subscriptions) {

        final Set<String> allSubscribedTopics = new HashSet<>();
        final Map<String, List<String>> topicSubscriptions = new HashMap<>();
        for (Map.Entry<String, Subscription> subscriptionEntry : subscriptions.groupSubscription().entrySet()) {
            List<String> topics = subscriptionEntry.getValue().topics();
            allSubscribedTopics.addAll(topics);
            topicSubscriptions.put(subscriptionEntry.getKey(), topics);
        }

        final Map<String, List<TopicPartitionLag>> topicLags = readTopicPartitionLags(metadata, allSubscribedTopics);
        Map<String, List<TopicPartition>> rawAssignments = assign(topicLags, topicSubscriptions);

        // this class has maintains no user data, so just wrap the results
        Map<String, Assignment> assignments = new HashMap<>();
        for (Map.Entry<String, List<TopicPartition>> assignmentEntry : rawAssignments.entrySet()) {
            assignments.put(assignmentEntry.getKey(), new Assignment(assignmentEntry.getValue()));
        }
        return new GroupAssignment(assignments);
    }



    static Map<String, List<TopicPartition>> assign(
            Map<String, List<TopicPartitionLag>> partitionLagPerTopic,
            Map<String, List<String>> subscriptions
    ) {
        // each memmber/consumer to its propsective assignment
        final Map<String, List<TopicPartition>> assignment = new HashMap<>();
        for (String memberId : subscriptions.keySet()) {
            assignment.put(memberId, new ArrayList<>());
        }
        //for each topic assign call assigntopic to perform lag-aware assignment per topic
        final Map<String, List<String>> consumersPerTopic = consumersPerTopic(subscriptions);
        for (Map.Entry<String, List<String>> topicEntry : consumersPerTopic.entrySet()) {
            assignTopic(
                    assignment,
                    //topic
                    topicEntry.getKey(),
                    //consumers
                    topicEntry.getValue(),
                    partitionLagPerTopic.getOrDefault(topicEntry.getKey(), Collections.emptyList())
            );
        }

        return assignment;

    }


    private static void assignTopic(
            final Map<String, List<TopicPartition>> assignment,
            final String topic,
            final List<String> consumers,
            final List<TopicPartitionLag> partitionLags
    ) {

        if (consumers.isEmpty()) {
            return;
        }
         //many consumers could be per topic
        //lag for each consumer
        // Track total lag assigned to each consumer (for the current topic)
        final Map<String, Long> consumerTotalLags = new HashMap<>(consumers.size());
        for (String memberId : consumers) {
            consumerTotalLags.put(memberId, 0L);
        }

        // Track total number of partitions assigned to each consumer (for the current topic)
        final Map<String, Integer> consumerTotalPartitions = new HashMap<>(consumers.size());
        for (String memberId : consumers) {
            consumerTotalPartitions.put(memberId, 0);
        }

        // Assign partitions in descending order of lag, then ascending by partition
        partitionLags.sort((p1, p2) -> {
            // If lag is equal, lowest partition id first
            if (p1.getLag() == p2.getLag()) {
                return Integer.compare(p1.getPartition(), p2.getPartition());
            }
            // Highest lag first
            return Long.compare(p2.getLag(), p1.getLag());
        });

        for (TopicPartitionLag partition : partitionLags) {

            // Assign to the consumer with least number of partitions, then smallest total lag, then smallest id

            // returns the consumer with lowest assigned partitions, if all assigned partitions equal returns the min total lag
            final String memberId = Collections
                    .min(
                            consumerTotalLags.entrySet(),
                            (c1, c2) -> {

                                // Lowest partition count first
                                final int comparePartitionCount = Integer.compare(consumerTotalPartitions.get(c1.getKey()),
                                        consumerTotalPartitions.get(c2.getKey()));
                                if (comparePartitionCount != 0) {
                                    return comparePartitionCount;
                                }

                                // If partition count is equal, lowest total lag first
                                final int compareTotalLags = Long.compare(c1.getValue(), c2.getValue());
                                if (compareTotalLags != 0) {
                                    return compareTotalLags;
                                }

                                // If total lag is equal, lowest consumer id first
                                return c1.getKey().compareTo(c2.getKey());

                            }
                    )
                    .getKey();
            assignment.get(memberId).add(new TopicPartition(partition.getTopic(), partition.getPartition()));
            consumerTotalLags.put(memberId, consumerTotalLags.getOrDefault(memberId, 0L) + partition.getLag());
            consumerTotalPartitions.put(memberId, consumerTotalPartitions.getOrDefault(memberId, 0) + 1);

            LOGGER.info(
                    "Assigned partition {}-{} to consumer {}.  partition_lag={}, consumer_current_total_lag={}",
                    partition.getTopic(),
                    partition.getPartition(),
                    memberId,
                    partition.getLag(),
                    consumerTotalLags.get(memberId)
            );

        }

        // Log assignment and total consumer lags for current topic
        if (LOGGER.isDebugEnabled()) {

            final StringBuilder topicSummary = new StringBuilder();
            for (Map.Entry<String, Long> entry : consumerTotalLags.entrySet()) {

                final String memberId = entry.getKey();
                topicSummary.append(
                        String.format(
                                "\t%s (total_lag=%d)\n",
                                memberId,
                                consumerTotalLags.get(memberId)
                        )
                );

                for (TopicPartition tp : assignment.getOrDefault(memberId, Collections.emptyList())) {
                    topicSummary.append(String.format("\t\t%s\n", tp));
                }

            }

            LOGGER.info(
                    "Assignment for {}:\n{}",
                    topic,
                    topicSummary
            );

        }

    }


    private Map<String, List<TopicPartitionLag>> readTopicPartitionLags(
            final Cluster metadata,
            final Set<String> allSubscribedTopics
    ) {
        if (metadataConsumer == null) {
            metadataConsumer = new KafkaConsumer<>(metadataConsumerProps);
        }
       // metadataConsumer.enforceRebalance();
        final Map<String, List<TopicPartitionLag>> topicPartitionLags = new HashMap<>();
        for (String topic : allSubscribedTopics) {

            final List<PartitionInfo> topicPartitionInfo = metadata.partitionsForTopic(topic);
            if (topicPartitionInfo != null && !topicPartitionInfo.isEmpty()) {

                final List<TopicPartition> topicPartitions = topicPartitionInfo.stream().map(
                        (PartitionInfo p) -> new TopicPartition(p.topic(), p.partition())
                ).collect(Collectors.toList());

                topicPartitionLags.put(topic, new ArrayList<>());

                // Get begin/end offset in each partition
                final Map<TopicPartition, Long> topicBeginOffsets = metadataConsumer.beginningOffsets(topicPartitions);
                final Map<TopicPartition, Long> topicEndOffsets = metadataConsumer.endOffsets(topicPartitions);
                //get last committed offset
                Map<TopicPartition, OffsetAndMetadata> partitionMetadata = metadataConsumer.committed(new HashSet<>(topicPartitions));
                // Determine lag for each partition
                for (TopicPartition partition : topicPartitions) {

                    final String autoOffsetResetMode = consumerGroupProps
                            .getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
                    final long lag = computePartitionLag(
                            partitionMetadata.get(partition),
                            topicBeginOffsets.getOrDefault(partition, 0L),
                            topicEndOffsets.getOrDefault(partition, 0L),
                            autoOffsetResetMode
                    );
                    topicPartitionLags.get(topic).add(new TopicPartitionLag(topic, partition.partition(), lag));
                }
            } else {
                LOGGER.info("Skipping assignment for topic {} since no metadata is available", topic);
            }
        }
        return topicPartitionLags;
    }



    static long computePartitionLag(
            final OffsetAndMetadata partitionMetadata,
            final long beginOffset,
            final long endOffset,
            final String autoOffsetResetMode
    ) {

        final long nextOffset;
        if (partitionMetadata != null) {

            nextOffset = partitionMetadata.offset();

        } else {

            // No committed offset for this partition, set based on auto.offset.reset
            if (autoOffsetResetMode.equalsIgnoreCase("latest")) {
                nextOffset = endOffset;
            } else {
                // assume earliest
                nextOffset = beginOffset;
            }

        }

        // The max() protects against the unlikely case when reading the partition end offset fails
        // but reading the last committed offsets succeeds
        return Long.max(endOffset - nextOffset, 0L);
    }


    private static Map<String, List<String>> consumersPerTopic(Map<String, List<String>> subscriptions) {

        final Map<String, List<String>> consumersPerTopic = new HashMap<>();
        for (Map.Entry<String, List<String>> subscriptionEntry : subscriptions.entrySet()) {

            final String consumerId = subscriptionEntry.getKey();
            for (String topic : subscriptionEntry.getValue()) {

                List<String> topicConsumers = consumersPerTopic.computeIfAbsent(topic, k -> new ArrayList<>());
                topicConsumers.add(consumerId);

            }
        }

        return consumersPerTopic;

    }


    static class TopicPartitionLag {

        private final String topic;
        private final int partition;
        private final long lag;

        TopicPartitionLag(String topic, int partition, long lag) {
            this.topic = topic;
            this.partition = partition;
            this.lag = lag;
        }

        String getTopic() {
            return topic;
        }

        int getPartition() {
            return partition;
        }

        long getLag() {
            return lag;
        }

    }

}