package com.gaeainfo.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;

/**
 * kafka监听类
 */
public class Listener {
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());


    @KafkaListener(id = "test1", topics = {"newtest"}, groupId = "1111")
    public void listen(ConsumerRecord<?, ?> record) {
        logger.info("kafka的keyAAAAA  test: " + record.key());
        logger.info("kafka的value test: " + record.value().toString());
    }

    @KafkaListener(id = "test2", topics = {"newtest2"}, groupId = "333")
    public void listen2(ConsumerRecord<?, ?> record) {
        logger.info("newtest2 AAAAAAAAA 333: " + record.key());
        logger.info("kafka的value 333: " + record.value().toString());
    }

    /**
     * 默认groupid和id是一样的，所以不能设置id
     *
     * @param record
     */
    @KafkaListener(topics = {"newtest2"}, containerFactory = "kafkaListenerContainerFactory2")
    public void listen3(ConsumerRecord<?, ?> record) {
        logger.info("newtest2 AAAAAAAAA 44444: " + record.key());
        logger.info("kafka的value 333: " + record.value().toString());
    }

}