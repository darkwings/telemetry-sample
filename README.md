#Telemetry Sample

To start with stream

    java -Dstart.stream=true -jar telemetry-sample-0.0.1-SNAPSHOT.jar
    java -Dstart.stream=true -Dserver.port=8070 -Devent.sdk.state.store.dir=/tmp/telemetry-1 -jar telemetry-sample-0.0.1-SNAPSHOT.jar

To start only as a producer

    java -Dstart.stream=false -Dserver.port=8010 -jar telemetry-sample-0.0.1-SNAPSHOT.jar

To generate messages

    curl -X POST http://localhost:8010/generate