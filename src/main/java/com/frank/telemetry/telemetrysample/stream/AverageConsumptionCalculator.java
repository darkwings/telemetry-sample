package com.frank.telemetry.telemetrysample.stream;

import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import it.frank.telemetry.tracking.FuelConsumption;
import it.frank.telemetry.tracking.FuelConsumptionAverage;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static java.lang.String.format;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.streams.Topology.AutoOffsetReset.LATEST;
import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;

/**
 * @author ftorriani
 */
@Component
@Slf4j
public class AverageConsumptionCalculator {

    private static final String STORE_NAME = "eventstore";

    @Value("${streams.application.id}")
    private String streamsApplicationId;

    @Value("${streams.application.id}")
    private String applicationName;

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

    @PostConstruct
    public void init() {
        log.info( "Start stream: {}", this.startStream );
        if ( startStream ) {
            kafkaStreams = createTopology( consumptionTopic, consumptionTopicAvg );
            kafkaStreams.setUncaughtExceptionHandler(exception -> {
                log.error("Streams exception ", exception);
                return REPLACE_THREAD;
            });
            kafkaStreams.setStateListener( ( newState, oldState ) -> {
                log.info( "Setting new state {} (old was {})", newState, oldState );
            } );
            kafkaStreams.start();
            log.info( "Stream started!!!" );
            Runtime.getRuntime().addShutdownHook( new Thread( kafkaStreams::close ) );
        }
    }

    private KafkaStreams createTopology( String inputTopic, String outputTopic ) {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        SpecificAvroSerde<FuelConsumption> inputSerde = new SpecificAvroSerde<>();
        inputSerde.configure( singletonMap( SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl ),
                false );

        SpecificAvroSerde<FuelConsumptionAverage> averageSerde = new SpecificAvroSerde<>();
        averageSerde.configure( singletonMap( SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl ),
                false );

        KStream<String, FuelConsumption> stream = streamsBuilder
                .stream( inputTopic, Consumed.with( Serdes.String(), inputSerde ).withOffsetResetPolicy( LATEST ) );

        stream
                // Just log
                .peek( ( key, value ) -> log.debug( "incoming message: {} {}", key, value ) )
                // Group by key
                .groupByKey()
                .windowedBy( TimeWindows.of( Duration.ofMinutes( 10 ) ) )
                .aggregate( avgInitializer, ( key, sensorData, avg ) -> {
                    avg.setVehicleId( sensorData.getVehicleId() );
                    avg.setKey( sensorData.getVehicleId() );
                    avg.setNumberOfRecords( avg.getNumberOfRecords() + 1 );
                    avg.setConsumptionTotal( avg.getConsumptionTotal() +
                            sensorData.getConsumption() );
                    avg.setConsumptionAvg( avg.getConsumptionTotal() /
                            avg.getNumberOfRecords() );
                    return avg;
                }, Materialized.<String, FuelConsumptionAverage, WindowStore<Bytes, byte[]>>as( STORE_NAME )
                        .withValueSerde( averageSerde )
                        .withRetention( Duration.ofDays( 1 ) ) )
                .toStream()
                // Select new key (vehicleId). The windowing process emits a Windowed<String> keyed object
                .selectKey( ( k, v ) -> v.getVehicleId() )
                // Just log
                .peek( ( key, value ) -> log.debug( "Windowed aggregation, key {} and value {}", key, value ) )
                //publish our results to avg topic
                .to( outputTopic, Produced.with( Serdes.String(), averageSerde ) );

        return new KafkaStreams( streamsBuilder.build(), props() );
    }

    private String vehicleId( FuelConsumption value ) {
        return value.getVehicleId();
    }

    Initializer<FuelConsumptionAverage> avgInitializer = () -> {
        FuelConsumptionAverage average = new FuelConsumptionAverage();
        average.setConsumptionTotal( 0.0 );
        average.setNumberOfRecords( 0L );
        return average;
    };

    private Properties props() {
        Properties props = new Properties();
        props.put( StreamsConfig.APPLICATION_ID_CONFIG, streamsApplicationId );
        props.put( StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers );

        props.put( StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "1000" );
        props.put( StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "5000" );

        // Exactly once processing!!
        props.put( StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE );

        // Ser/Des and schema registry
        props.put( StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName() );
        props.put( StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName() );
        props.put( KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl );

        props.put( StreamsConfig.STATE_DIR_CONFIG, stateStoreDir);

        props.put( ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest" );

        // https://github.com/confluentinc/schema-registry/pull/680
        // MONOSCHEMA
//        props.put( "value.subject.name.strategy", "io.confluent.kafka.serializers.subject.TopicRecordNameStrategy" );
        return props;
    }
}
