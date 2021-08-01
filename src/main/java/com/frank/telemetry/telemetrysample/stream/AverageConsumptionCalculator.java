package com.frank.telemetry.telemetrysample.stream;

import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import it.frank.telemetry.tracking.FuelConsumption;
import it.frank.telemetry.tracking.FuelConsumptionAverage;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static java.time.Duration.*;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.streams.Topology.AutoOffsetReset.LATEST;
import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;

/**
 * @author ftorriani
 */
@Component
@Log4j2
public class AverageConsumptionCalculator {

    private static final String STORE_NAME = "telemetry-store";

    @Value("${streams.application.id}")
    private String streamsApplicationId;

    @Value("${event.sdk.bootstrap.servers}")
    private String bootstrapServers;

    @Value("${event.sdk.schema.registry.url}")
    private String schemaRegistryUrl;

    @Value("${topic.fuel-consumption}")
    private String consumptionTopic;

    @Value("${topic.fuel-consumption-avg}")
    private String consumptionTopicAvg;

    @Value("${start.stream:false}")
    private boolean startStream;

    @Value("${event.sdk.state.store.dir:/tmp/telemetry-0}")
    private String stateStoreDir;

    private KafkaStreams kafkaStreams;

    static class RestoreListener implements StateRestoreListener {
        @Override
        public void onRestoreStart(TopicPartition topicPartition, String storeName, long startingOffset, long endingOffset) {
            log.info("Started restore of {} [tp: {}, from: {}, to: {}]", storeName, topicPartition,
                    startingOffset, endingOffset);
        }

        @Override
        public void onBatchRestored(TopicPartition topicPartition, String storeName, long batchEndOffset, long numRestored) {
            log.info("Batch restored of {} [tp: {}, end offset: {}, restored: {}]", storeName, topicPartition,
                    batchEndOffset, numRestored);
        }

        @Override
        public void onRestoreEnd(TopicPartition topicPartition, String storeName, long totalRestored) {
            // Logs anything
        }
    }

    @PostConstruct
    public void init() {
        log.info("Start stream: {}", this.startStream);
        if (startStream) {
            kafkaStreams = createTopology(consumptionTopic, consumptionTopicAvg);
            kafkaStreams.setUncaughtExceptionHandler(exception -> {
                log.error("Streams exception ", exception);
                return REPLACE_THREAD;
            });
            kafkaStreams.setGlobalStateRestoreListener(new RestoreListener());
            kafkaStreams.setStateListener((newState, oldState) -> {
                log.info("Setting new state {} (old was {})", newState, oldState);
                if (newState == KafkaStreams.State.REBALANCING) {
                    // Do anything that's necessary to manage rebalance
                    log.info("Rebalance in progress");
                }
            });
            kafkaStreams.start();
            log.info("Stream started!!!");
            Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
        }
    }

    private KafkaStreams createTopology(String inputTopic, String outputTopic) {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        SpecificAvroSerde<FuelConsumption> inputSerde = new SpecificAvroSerde<>();
        inputSerde.configure(singletonMap(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl),
                false);

        SpecificAvroSerde<FuelConsumptionAverage> averageSerde = new SpecificAvroSerde<>();
        averageSerde.configure(singletonMap(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl),
                false);

        KStream<String, FuelConsumption> stream = streamsBuilder
                .stream(inputTopic, Consumed.with(Serdes.String(), inputSerde)
                        .withOffsetResetPolicy(LATEST)
                        .withTimestampExtractor(new FuelConsumptionTimestampExtractor()));

        // Control compaction of changelog topic
        Map<String, String> topicConfigs = new HashMap<>();
        topicConfigs.put("segment.bytes", "536870912");
        topicConfigs.put("min.cleanable.dirty.ratio", "0.3");

        stream
                .peek((key, value) -> log.debug("incoming message: {} {}", key, value))
                .groupByKey()
                .windowedBy(TimeWindows.of(ofMinutes(5)).grace(ofSeconds(20)))
                .aggregate(avgInitializer, (key, sensorData, avg) -> {
                    avg.setVehicleId(sensorData.getVehicleId());
                    avg.setKey(sensorData.getVehicleId());
                    avg.setNumberOfRecords(avg.getNumberOfRecords() + 1);
                    avg.setConsumptionTotal(avg.getConsumptionTotal() + sensorData.getConsumption());
                    avg.setConsumptionAvg(avg.getConsumptionTotal() / avg.getNumberOfRecords());
                    return avg;
                }, Materialized.<String, FuelConsumptionAverage, WindowStore<Bytes, byte[]>>as(STORE_NAME)
                        .withValueSerde(averageSerde)
                        .withLoggingEnabled(topicConfigs)
                        .withRetention(ofMinutes(15)))
                .toStream()
                // Select new key (vehicleId). The windowing process emits a Windowed<String> keyed object
                .selectKey((k, v) -> v.getVehicleId())
                // Just log
                .peek((key, value) -> log.debug("Windowed aggregation, key {} and value {}", key, value))
                //publish our results to avg topic
                .to(outputTopic, Produced.with(Serdes.String(), averageSerde));

        return new KafkaStreams(streamsBuilder.build(), props());
    }

    private String vehicleId(FuelConsumption value) {
        return value.getVehicleId();
    }

    Initializer<FuelConsumptionAverage> avgInitializer = () -> {
        FuelConsumptionAverage average = new FuelConsumptionAverage();
        average.setConsumptionTotal(0.0);
        average.setNumberOfRecords(0L);
        return average;
    };

    private Properties props() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, streamsApplicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // Limits the deduplication cache to 50Kb
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "50000");

        // commits every 20 seconds on the destination topic / changelog
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "20000");

        // Maximize options to recover from a standby replica of the changelog
        props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 2);

        // Exactly once processing!!
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        // Ser/Des and schema registry
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName());
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        // The state store directory
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateStoreDir);

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        // https://github.com/confluentinc/schema-registry/pull/680
        // MONOSCHEMA
//        props.put( "value.subject.name.strategy", "io.confluent.kafka.serializers.subject.TopicRecordNameStrategy" );
        return props;
    }
}
