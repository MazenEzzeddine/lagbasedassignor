package org.hps;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MonitoringThread extends Thread {

    private static final Logger log = LoggerFactory.getLogger(LagBasedPartitionAssignor.class);


    // run() method contains the code that is executed by the thread.

        private KafkaConsumer consumer;

        MonitoringThread(KafkaConsumer cons) {
            this.consumer = cons;
        }

        @Override
        public void run()  {
            log.info ("Mazen Inside : " + Thread.currentThread().getName());
            try {
                Thread.sleep(3000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

