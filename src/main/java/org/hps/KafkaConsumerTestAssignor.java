package org.hps;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.header.Header;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerTestAssignor {
    private static final Logger log = LogManager.getLogger(KafkaConsumerTestAssignor.class);

    public static void main(String[] args) throws InterruptedException {
        KafkaConsumerConfig config = KafkaConsumerConfig.fromEnv();

        log.info(KafkaConsumerConfig.class.getName() + ": {}", config.toString());

        Properties props = KafkaConsumerConfig.createProperties(config);
        int receivedMsgs = 0;

       // props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, StickyAssignor.class.getName());
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, LagBasedPartitionAssignor.class.getName());
        //props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

        boolean commit = !Boolean.parseBoolean(config.getEnableAutoCommit());
        KafkaConsumer consumer = new KafkaConsumer(props);
        consumer.subscribe(Collections.singletonList(config.getTopic()));


        while (receivedMsgs < config.getMessageCount()) {

          //  consumer.enforceRebalance();

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
            for (ConsumerRecord<String, String> record : records) {
                log.info("Received message:");
                log.info("\tpartition: {}", record.partition());
                log.info("\toffset: {}", record.offset());
                log.info("\tvalue: {}", record.value());
                if (record.headers() != null) {
                    log.info("\theaders: ");
                    for (Header header : record.headers()) {
                        log.info("\t\tkey: {}, value: {}", header.key(), new String(header.value()));
                    }
                }
                receivedMsgs++;
            }
            if (commit) {
                consumer.commitSync();
            }

            log.info("Sleeping for {} milliseconds", config.getSleep());
            Thread.sleep(Long.parseLong(config.getSleep()));
        }
        log.info("Received {} messages", receivedMsgs);
    }
}




