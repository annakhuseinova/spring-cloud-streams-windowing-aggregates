package com.annakhuseinova.springcloudstreamswindowingaggregates.services;

import com.annakhuseinova.springcloudstreamswindowingaggregates.bindings.ClickListenerBinding;
import com.annakhuseinova.springcloudstreamswindowingaggregates.model.UserClick;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

@Service
@Slf4j
@EnableBinding(ClickListenerBinding.class)
public class ClickListenerService {

    @StreamListener("click-input-channel")
    public void process(KStream<String, UserClick> input){
        input.peek((key, value)-> log.info("Key = " + key + " Created Time: = " + Instant.ofEpochMilli(
                value.getCreatedTime()).atOffset(ZoneOffset.UTC)
        )).groupByKey()
                // We specify which kind of window we want here (session window). We specify 5 minutes which means
                //
                .windowedBy(SessionWindows.with(Duration.ofMinutes(5)))
                .count().toStream()
        .foreach((key, value)-> log.info("UserID: " + key.key() + " Window start: " + Instant.ofEpochMilli(key.window()
        .start()).atOffset(ZoneOffset.UTC) + "Window end: " + Instant.ofEpochMilli(key.window().end())
                .atOffset(ZoneOffset.UTC) + " Count: " + value + " Window#: " + key.window().hashCode()));
    }
}
