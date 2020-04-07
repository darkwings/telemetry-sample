package com.frank.telemetry.telemetrysample;

import com.facilitylive.cloud.events.sdk.autoconfiguration.FlEventsAutoConfiguration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;

@SpringBootApplication
@Import( { FlEventsAutoConfiguration.class } )
public class TelemetrySampleApplication {

	public static void main(String[] args) {
		SpringApplication.run(TelemetrySampleApplication.class, args);
	}

}
