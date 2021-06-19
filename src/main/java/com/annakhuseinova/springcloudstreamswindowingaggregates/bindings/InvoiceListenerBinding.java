package com.annakhuseinova.springcloudstreamswindowingaggregates.bindings;

import com.annakhuseinova.springcloudstreamswindowingaggregates.model.SimpleInvoice;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.cloud.stream.annotation.Input;

public interface InvoiceListenerBinding {

    @Input("invoice-input-channel")
    KStream<String, SimpleInvoice> invoiceInputStream();
}
