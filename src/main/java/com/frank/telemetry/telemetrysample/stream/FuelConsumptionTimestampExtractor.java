package com.frank.telemetry.telemetrysample.stream;

import it.frank.telemetry.tracking.FuelConsumption;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

/**
 * Custom timestamp extractor that use event time
 */
public class FuelConsumptionTimestampExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        FuelConsumption fuelConsumption = (FuelConsumption) record.value();
        if (fuelConsumption != null && fuelConsumption.getTimestampMs() != null) {
            return fuelConsumption.getTimestampMs();
        }
        return partitionTime;
    }
}
