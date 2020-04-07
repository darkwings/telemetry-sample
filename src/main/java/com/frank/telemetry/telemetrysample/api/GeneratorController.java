package com.frank.telemetry.telemetrysample.api;

import com.facilitylive.cloud.events.sdk.FlEvents;
import it.frank.telemetry.tracking.FuelConsumption;
import it.frank.telemetry.tracking.VehiclePosition;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Clock;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author ftorriani
 */
@RestController
@Slf4j
public class GeneratorController {

    @Autowired
    private FlEvents flEvents;

    @PostMapping("/generate")
    public ResponseEntity<String> generator() {

        ExecutorService executorService = Executors.newFixedThreadPool( 2 );
        executorService.submit( new Generator() );

        return new ResponseEntity<>( "OK", HttpStatus.OK );
    }

    private class Generator implements Runnable {

        double latitude = 45.00000;
        double longitude = 6.00000;

        double consumption = 5.00;

        private String[] vehiclesId = new String[]{ "v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8", "v9",
                "v10" };


        @Override
        public void run() {

            Random random = new Random();
            try {
                while ( true ) {
                    long time = Clock.systemUTC().millis();
                    int index = random.nextInt( 10 );
                    String vehicleId = vehiclesId[ index ];
                    if ( time % 2 == 0 ) {
                        latitude = latitude + 0.00001;
                        longitude = longitude + 0.00001;
                        VehiclePosition position = VehiclePosition.newBuilder()
                                .setEventId( UUID.randomUUID().toString() )
                                .setVehicleId( vehicleId )
                                .setKey( vehicleId )
                                .setTimestampMs( time )
                                .setLatitude( latitude )
                                .setLongitude( longitude )
                                .build();
                        flEvents.publish( "tenant1", position );
                    }
                    else {
                        int factor = random.nextInt( 40 );
                        boolean plus = factor % 2 == 0;
                        double instantConsumption = consumption + (plus ? 0.1 * factor : -0.1 * factor);
                        FuelConsumption consumption = FuelConsumption.newBuilder()
                                .setEventId( UUID.randomUUID().toString() )
                                .setVehicleId( vehicleId )
                                .setKey( vehicleId )
                                .setTimestampMs( time )
                                .setConsumption( instantConsumption )
                                .build();
                        flEvents.publish( "tenant1", consumption );
                    }
                    Thread.sleep( 200L );
                }
            }
            catch ( Exception e ) {
                log.error( "Error", e );
            }
        }
    }
}
