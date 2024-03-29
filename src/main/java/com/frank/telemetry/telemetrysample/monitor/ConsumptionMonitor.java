package com.frank.telemetry.telemetrysample.monitor;

import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import it.frank.telemetry.tracking.FuelConsumptionAverage;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.lang.String.format;
import static java.time.Duration.ofMillis;

/**
 * Implementazione di test di un possibile monitor per i consumi medi dei veicoli.
 * <p>
 * <strong>DISCLAIMER: NON E' DA PRENDERE A MODELLO PER USO SERIO</strong>
 * <p>
 * Qui supponiamo di avere un solo consumer attivo su tutte le partizioni del topic fuel-consumption-avg,
 * per cui riceviamo tutti i dati.
 * <p>
 * Un'implementazione reale aggiornerebbe una cache o un database.
 */
@Component
@Log4j2
public class ConsumptionMonitor {

    @Value("${event.sdk.bootstrap.servers}")
    private String bootstrapServers;

    @Value("${start.stream:false}")
    private boolean startStream;

    @Value("${topic.fuel-consumption-avg}")
    private String consumptionTopicAvg;

    @Value("${event.sdk.schema.registry.url}")
    private String schemaRegistryUrl;

    private Consumer<String, FuelConsumptionAverage> consumer;

    private final ExecutorService service = Executors.newFixedThreadPool(1);

    Map<String, Double> monitor = new HashMap<>();

    public Map<String, Double> monitor() {
        return monitor;
    }

    @PostConstruct
    public void init() {

        if (!startStream) {
            // Basic consumer configuration
            log.info("Starting consumption monitor");
            consumer = new KafkaConsumer<>(props());
            consumer.subscribe(Collections.singletonList(consumptionTopicAvg));
            service.submit(() -> {
                try {
                    while (true) {
                        ConsumerRecords<String, FuelConsumptionAverage> records = consumer.poll(ofMillis(100));
                        for (ConsumerRecord<String, FuelConsumptionAverage> record : records) {
                            log.debug("topic = {}, partition = {}, offset = {}, customer = {}, country = {}\n",
                                    record.topic(), record.partition(), record.offset(),
                                    record.key(), record.value());
                            FuelConsumptionAverage average = record.value();
                            monitor.put(average.getVehicleId(), average.getConsumptionAvg());
                        }
                        consumer.commitAsync((offsets, e) -> {
                            if (e != null) {
                                log.error(format("Commit failed for offsets %s", offsets), e);
                            }
                        });
                    }
                }
                catch (WakeupException e) {
                    log.info("Woke up. Stopping loop");
                }
                catch (Exception e) {
                    log.error("Unexpected error", e);
                }
                finally {
                    consumer.close();
                }
            });


            Runtime.getRuntime().addShutdownHook(new Thread(() -> consumer.wakeup()));
        }
    }



    private Properties props() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "monitor.avg");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SpecificAvroDeserializer.class.getName());
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        return props;
    }
}
