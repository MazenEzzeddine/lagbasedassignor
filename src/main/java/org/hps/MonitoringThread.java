package org.hps;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class MonitoringThread extends Thread implements Configurable {

    private static final Logger log = LoggerFactory.getLogger(MonitoringThread.class);


    // run() method contains the code that is executed by the thread.

    private KafkaConsumer<String, String> metadataConsumer;

    private Cluster metadata;

    private Properties consumerGroupProps;
    private Properties metadataConsumerProps;
    MonitoringThread(Cluster meta) {

        this.metadata = meta;
    }

    @Override
    public void run() {

        createDirectConsumer();


        while (true) {


            log.info(" Mazen Inside : " + Thread.currentThread().getName());


            final List<PartitionInfo> topicPartitionInfo = metadata.partitionsForTopic("testtopic2");
            if (topicPartitionInfo != null && !topicPartitionInfo.isEmpty()) {

                final List<TopicPartition> topicPartitions = topicPartitionInfo.stream().map(
                        (PartitionInfo p) -> new TopicPartition(p.topic(), p.partition())
                ).collect(Collectors.toList());


                // Get begin/end offset in each partition
                final Map<TopicPartition, Long> topicBeginOffsets = metadataConsumer.beginningOffsets(topicPartitions);
                final Map<TopicPartition, Long> topicEndOffsets = metadataConsumer.endOffsets(topicPartitions);
                //get last committed offset
                Map<TopicPartition, OffsetAndMetadata> partitionMetadata = metadataConsumer.committed(new HashSet<>(topicPartitions));

                for (TopicPartition partition : topicPartitions) {
                    log.info("partition {} has begin offsets {}", partition, topicBeginOffsets.get(partition));
                    log.info("partition {} has end offsets {}", partition, topicEndOffsets.get(partition));
                    log.info("partition {} has committed  offsets {}", partition, partitionMetadata.get(partition).offset());
                }
                try {
                    Thread.sleep(10000);
                    log.info(" Mazen Inside : " + metadataConsumer.toString());

                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        }
    }


    public void createDirectConsumer(){
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "my-cluster-kafka-bootstrap:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "testgroup2");

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        // props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        metadataConsumer = new KafkaConsumer<String, String>(props);


        metadataConsumer.subscribe(Collections.singletonList("testtopic2"));

    }





    @Override
    public void configure(Map<String, ?> configs) {
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

        log.info("groupId {}", groupId);

        // Create a new consumer that can be used to get lag metadata for the consumer group
        metadataConsumerProps = new Properties();
        metadataConsumerProps.putAll(consumerGroupProps);
        metadataConsumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        final String clientId = groupId + ".assignor1";
        metadataConsumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);

        log.info(
                "Configured a meta consumer  with values:\n"
                        + "\tgroup.id = {}\n"
                        + "\tclient.id = {}\n",
                groupId,
                clientId
        );

        log.info("creating the metadataconsumer inside the configure in the thread ");



    }
}



