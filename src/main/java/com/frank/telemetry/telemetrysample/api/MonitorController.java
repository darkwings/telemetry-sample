package com.frank.telemetry.telemetrysample.api;

import com.frank.telemetry.telemetrysample.monitor.ConsumptionMonitor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
public class MonitorController {

    @Autowired
    private ConsumptionMonitor consumptionMonitor;

    @GetMapping(value = "/monitor", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public ResponseEntity<Map<String, Double>> monitor() {
        return new ResponseEntity<>(consumptionMonitor.monitor(), HttpStatus.OK);
    }
}
