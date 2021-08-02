package com.frank.telemetry.telemetrysample.api;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author ftorriani
 */
@RestController
@Slf4j
public class GeneratorController {

    @Value("${topic.tracking}")
    private String trackingTopic;

    @Value("${topic.fuel-consumption}")
    private String consumptionTopic;

    @Autowired
    private PublisherSupport publisherSupport;

    @PostMapping("/generate")
    public ResponseEntity<String> generator() {
        publisherSupport.startGenerator();
        return new ResponseEntity<>("OK", HttpStatus.OK);
    }


}
