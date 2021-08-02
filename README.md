#Telemetry Sample

To start with stream processors active

    java -Dstart.stream=true -jar target/telemetry-sample-0.0.1-SNAPSHOT.jar
    java -Dstart.stream=true -Dserver.port=8070 -Devent.sdk.state.store.dir=/tmp/telemetry-1 -Dgroup.instance.id=telemetry-2 -jar target/telemetry-sample-0.0.1-SNAPSHOT.jar

To start only as a producer/monitor

    java -Dstart.stream=false -Dserver.port=8010 -Dmock.publisher.fleet.size=20 -jar target/telemetry-sample-0.0.1-SNAPSHOT.jar

To generate messages from the producer instance

    curl -X POST http://localhost:8010/generate

To monitor data

    curl http://localhost:8010/monitor