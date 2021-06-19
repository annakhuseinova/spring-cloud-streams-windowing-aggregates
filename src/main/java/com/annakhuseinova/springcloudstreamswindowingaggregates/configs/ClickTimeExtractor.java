package com.annakhuseinova.springcloudstreamswindowingaggregates.configs;

import com.annakhuseinova.springcloudstreamswindowingaggregates.model.UserClick;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@Slf4j
public class ClickTimeExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> consumerRecord, long previousRecordTime) {
        UserClick click = (UserClick) consumerRecord.value();
        log.info("Click Time: {}", click.getCreatedTime());
        return ((click.getCreatedTime() > 0) ? click.getCreatedTime() : previousRecordTime);
    }

    @Bean
    public TimestampExtractor userClickTimeExtractor(){
        return new ClickTimeExtractor();
    }
}
