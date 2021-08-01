package com.frank.telemetry.telemetrysample.api;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import it.frank.telemetry.tracking.FuelConsumption;
import it.frank.telemetry.tracking.VehiclePosition;
import lombok.extern.log4j.Log4j2;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.time.Clock;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.lang.Thread.sleep;

@Component
@Log4j2
public class PublisherSupport {

    @Value("${event.sdk.bootstrap.servers}")
    private String bootstrapServers;

    @Value("${event.sdk.schema.registry.url}")
    private String schemaRegistryUrl;

    @Value("${topic.tracking}")
    private String trackingTopic;

    @Value("${topic.fuel-consumption}")
    private String consumptionTopic;

    @Value("${mock.publisher.fleet.size:100}")
    private int fleetSize;

    private Producer<String, SpecificRecord> producer;

    @PostConstruct
    public void init() {
        log.info("Initializing embedded test producer");
        producer = new KafkaProducer<>(producerProperties());
    }

    public Properties producerProperties() {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());

        // props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, maxBlockMillis);

        // Retries / idempotency
        props.put(ProducerConfig.ACKS_CONFIG, "0");
        // props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMillis);
        // props.put(ProducerConfig.RETRIES_CONFIG, retries);
        // props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, maxInFlightRequestsPerConnections);
        // props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, idemPotentProducer);
        // props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, deliveryTimeoutMillis);
        // if (idemPotentProducer) {
        //     props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionId);
        // }

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        // if (multiSchema) {
        //    // https://github.com/confluentinc/schema-registry/pull/68
        //     props.put(KafkaAvroSerializerConfig.VALUE_SUBJECT_NAME_STRATEGY,
        //             TopicRecordNameStrategy.class.getName());
        // }

        return props;
    }

    public Generator generator() {
        return new Generator(producer, trackingTopic, consumptionTopic, fleetSize);
    }


    private static class Generator implements Runnable {

        double latitude = 45.00000;
        double longitude = 6.00000;

        double consumption = 5.00;

        private final List<String> vehicleIds;

        private final Producer<String, SpecificRecord> producer;
        private final String trackingTopic;
        private final String consumptionTopic;

        private final int fleetSize;

        public Generator(Producer<String, SpecificRecord> producer, String trackingTopic,
                         String consumptionTopic, int fleetSize) {
            vehicleIds = IntStream.of(0, fleetSize).mapToObj(v -> "v" + v).collect(Collectors.toList());

            this.producer = producer;
            this.trackingTopic = trackingTopic;
            this.consumptionTopic = consumptionTopic;
            this.fleetSize = fleetSize;
        }

        @Override
        public void run() {

            int positionC = 0;
            int consumptionC = 0;
            Random random = new Random();
            try {
                while (true) {
                    long time = Clock.systemUTC().millis();
                    int index = random.nextInt(fleetSize);
                    String vehicleId = vehicleIds.get(index);
                    if (time % 2 == 0) {
                        positionC++;
                        if (positionC % 100 == 0) {
                            log.info("Published {} position update", positionC);
                        }
                        latitude = latitude + 0.00001;
                        longitude = longitude + 0.00001;
                        VehiclePosition position = VehiclePosition.newBuilder()
                                .setEventId(UUID.randomUUID().toString())
                                .setVehicleId(vehicleId)
                                .setTimestampMs(time)
                                .setLatitude(latitude)
                                .setLongitude(longitude)
                                .setKey(vehicleId)
                                .build();
                        producer.send(new ProducerRecord<>(trackingTopic, vehicleId, position));
                    } else {
                        consumptionC++;
                        if (consumptionC % 100 == 0) {
                            log.info("Published {} consumption update", consumptionC);
                        }
                        int factor = random.nextInt(40);
                        boolean plus = factor % 2 == 0;
                        double instantConsumption = consumption + (plus ? 0.1 * factor : -0.1 * factor);
                        FuelConsumption consumption = FuelConsumption.newBuilder()
                                .setEventId(UUID.randomUUID().toString())
                                .setVehicleId(vehicleId)
                                .setTimestampMs(time)
                                .setConsumption(instantConsumption)
                                .setKey(vehicleId)
                                .build();
                        producer.send(new ProducerRecord<>(consumptionTopic,
                                vehicleId, consumption));
                    }
                    sleep(50L);


                }
            } catch (Exception e) {
                log.error("Error", e);
            }
        }
    }
}
