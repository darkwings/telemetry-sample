package com.frank.telemetry.telemetrysample.api;

import com.frank.telemetry.telemetrysample.stream.AverageConsumptionCalculator;
import it.frank.telemetry.tracking.FuelConsumption;
import it.frank.telemetry.tracking.FuelConsumptionAverage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author ftorriani
 */

@RestController
public class QueryController {

    @Autowired
    private AverageConsumptionCalculator calculator;

    @GetMapping(value = "/average/{vehicleId}", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public ResponseEntity<FuelConsumptionAverage> get( @PathVariable("vehicleId") String vehicleId ) {
        return calculator.currentAverage( vehicleId ).map( avg -> new ResponseEntity<>( avg, HttpStatus.OK ) )
                .orElseGet( () -> new ResponseEntity<>( null, HttpStatus.NOT_FOUND ) );
    }
}
