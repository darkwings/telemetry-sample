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
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static java.lang.String.format;
import static java.util.Collections.singletonMap;

/**
 * @author ftorriani
 */
@Component
@Slf4j
public class AverageConsumptionCalculator {

    private static final String STORE_NAME = "eventstore";

    private final String streamsApplicationId;
    private final String bootstrapServers;
    private final String schemaRegistryUrl;
    private final String consumptionTopic;
    private final String consumptionTopicAvg;

    private final boolean startStream;

    private KafkaStreams kafkaStreams;

    public AverageConsumptionCalculator( @Value("${streams.application.id}") String streamsApplicationId,
                                         @Value("${event.sdk.bootstrap.servers}") String bootstrapServers,
                                         @Value("${event.sdk.schema.registry.url}") String schemaRegistryUrl,
                                         @Value("${start.stream:false}") boolean startStream,
                                         @Value( "${topic.fuel-consumption}" ) String consumptionTopic,
                                         @Value( "${topic.fuel-consumption-avg}" ) String consumptionTopicAvg ) {
        this.streamsApplicationId = streamsApplicationId;
        this.bootstrapServers = bootstrapServers;
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.consumptionTopic = consumptionTopic;
        this.consumptionTopicAvg = consumptionTopicAvg;
        this.startStream = startStream;
    }

    @PostConstruct
    public void init() {
        log.info( "Start stream: {}", this.startStream );
        if ( startStream ) {
            kafkaStreams = createTopology( consumptionTopic, consumptionTopicAvg );
            kafkaStreams.setUncaughtExceptionHandler( ( t, e ) -> {
                log.error( format( "Thread %s: caught error %s", t.getName(), e.getMessage() ), e );
            } );
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
                .stream( inputTopic, Consumed.with( Serdes.String(), inputSerde ) );

        stream.peek( ( key, value ) -> log.debug( "incoming message: {} {}", key, value ) )
//                .groupBy( ( key, value ) -> vehicleId( value ), Grouped.with( Serdes.String(), inputSerde ) )
                .groupByKey()
                // Consumo medio negli ultimi 10 minuti
                .windowedBy( TimeWindows.of( Duration.ofMinutes( 10 ) ) )
                // l'operatore aggregate() è stateful, ha bisogno di uno state store cui memorizzare la versione
                // precedente dell'aggregate, in questo caso l'oggetto FuelConsumptionAverage.
                .aggregate( avgInitializer, ( key, sensorFuelConsumption, fuelConsumptionAverage ) -> {
                    fuelConsumptionAverage.setVehicleId( sensorFuelConsumption.getVehicleId() );
                    fuelConsumptionAverage.setNumberOfRecords( fuelConsumptionAverage.getNumberOfRecords() + 1 );
                    fuelConsumptionAverage.setConsumptionTotal( fuelConsumptionAverage.getConsumptionTotal() +
                            sensorFuelConsumption.getConsumption() );
                    fuelConsumptionAverage.setConsumptionAvg( fuelConsumptionAverage.getConsumptionTotal() /
                            fuelConsumptionAverage.getNumberOfRecords() );
                    return fuelConsumptionAverage;
                }, Materialized.<String, FuelConsumptionAverage, WindowStore<Bytes, byte[]>>as( STORE_NAME )
                        .withValueSerde( averageSerde )
                        .withRetention( Duration.ofDays( 5 ) ) )
                .toStream()
                // Change back the key to vehicleId
                .selectKey( (k, v) -> v.getVehicleId() )
                .peek( ( key, value ) -> log.debug( "Windowed aggregation, key {} and value {}", key, value ) )
                //publish our results to avg topic
                .to( consumptionTopicAvg, Produced.with( Serdes.String(), averageSerde ) );

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
        props.put( ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest" );

        // we disable the cache to demonstrate all the "steps" involved in the transformation -
        // not recommended in prod
        // props.put( StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0" ); // TODO

        // Exactly once processing!!
        props.put( StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE );

        // Ser/Des and schema registry
        props.put( StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName() );
        props.put( StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName() );
        props.put( KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl );

        // https://github.com/confluentinc/schema-registry/pull/680
        // MONOSCHEMA
//        props.put( "value.subject.name.strategy", "io.confluent.kafka.serializers.subject.TopicRecordNameStrategy" );
        return props;
    }
}
