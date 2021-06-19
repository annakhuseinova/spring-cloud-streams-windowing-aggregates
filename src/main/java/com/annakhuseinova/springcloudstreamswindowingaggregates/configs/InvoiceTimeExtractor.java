package com.annakhuseinova.springcloudstreamswindowingaggregates.configs;

import com.annakhuseinova.springcloudstreamswindowingaggregates.model.SimpleInvoice;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class InvoiceTimeExtractor implements TimestampExtractor {

    /**
     * Every time you receive a message record from the Kafka topic, extract method will be invoked and it will give
     * you the message record
     *
     * You can also read metadata timestamp from the consumerRecord
     * */
    @Override
    public long extract(ConsumerRecord<Object, Object> consumerRecord, long previousRecordTime) {
        SimpleInvoice invoice = (SimpleInvoice) consumerRecord.value();
        return ((invoice.getCreatedTime() > 0) ? invoice.getCreatedTime(): previousRecordTime);
    }

    @Bean
    public TimestampExtractor invoiceTimesExtractor(){
        return  new InvoiceTimeExtractor();
    }
}
