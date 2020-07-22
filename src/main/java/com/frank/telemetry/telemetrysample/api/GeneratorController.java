package com.frank.telemetry.telemetrysample.api;

import com.facilitylive.cloud.events.sdk.FlEvents;
import it.frank.telemetry.tracking.FuelConsumption;
import it.frank.telemetry.tracking.VehiclePosition;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Clock;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.lang.Thread.sleep;

/**
 * @author ftorriani
 */
@RestController
@Slf4j
public class GeneratorController {

    @Autowired
    private FlEvents flEvents;

    @Value("${topic.tracking}")
    private String trackingTopic;

    @Value("${topic.fuel-consumption}")
    private String consumptionTopic;

    @PostMapping("/generate/{tenant}")
    public ResponseEntity<String> generator( @PathVariable("tenant") String tenant ) {

        ExecutorService executorService = Executors.newFixedThreadPool( 2 );
        executorService.submit( new Generator( tenant ) );

        return new ResponseEntity<>( "OK", HttpStatus.OK );
    }

    private class Generator implements Runnable {

        double latitude = 45.00000;
        double longitude = 6.00000;

        double consumption = 5.00;

        String tenant;

        public Generator( String tenant ) {
            this.tenant = tenant;
        }

        private String[] vehiclesId = new String[]{ "v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8", "v9",
                "v10" };

//        private Producer<String, SpecificRecord> producer;

        @Override
        public void run() {

            int positionC = 0;
            int consumptionC = 0;
            Random random = new Random();
            try {
                while ( true ) {
                    long time = Clock.systemUTC().millis();
                    int index = random.nextInt( 10 );
                    String vehicleId = vehiclesId[ index ];
                    if ( time % 2 == 0 ) {
                        positionC++;
                        if ( positionC % 100 == 0 ) {
                            log.info( "[{}] Published {} position update", tenant, positionC );
                        }
                        latitude = latitude + 0.00001;
                        longitude = longitude + 0.00001;
                        VehiclePosition position = VehiclePosition.newBuilder()
                                .setEventId( UUID.randomUUID().toString() )
                                .setVehicleId( vehicleId )
                                .setTimestampMs( time )
                                .setLatitude( latitude )
                                .setLongitude( longitude )
                                .setKey( vehicleId )
                                .setTenantId( tenant )
                                .build();
//                        producer.send( new ProducerRecord<>( trackingTopic, vehicleId, position ) );
                        flEvents.publish( tenant, position );
                    }
                    else {
                        consumptionC++;
                        if ( consumptionC % 100 == 0 ) {
                            log.info( "[{}] Published {} consumption update", tenant, consumptionC );
                        }
                        int factor = random.nextInt( 40 );
                        boolean plus = factor % 2 == 0;
                        double instantConsumption = consumption + ( plus ? 0.1 * factor : -0.1 * factor );
                        FuelConsumption consumption = FuelConsumption.newBuilder()
                                .setEventId( UUID.randomUUID().toString() )
                                .setVehicleId( vehicleId )
                                .setTimestampMs( time )
                                .setConsumption( instantConsumption )
                                .setKey( vehicleId )
                                .setTenantId( tenant )
                                .build();
//                        producer.send( new ProducerRecord<>( consumptionTopic,
//                                vehicleId, consumption ) );
                        flEvents.publish( tenant, consumption );
                    }
                    sleep( 50L );


                }
            }
            catch ( Exception e ) {
                log.error( "Error", e );
            }
        }
    }
}
