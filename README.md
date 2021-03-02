# Kafka Connect Pelion


kafka-connect-pelion is a Source and Sink [Kafka Connector](http://kafka.apache.org/documentation.html#connect)
for interacting with Pelion IoT platform.

### Source Connector Example config
```
name=connect-pelion-source

# kafka-connect-specific properties
connector.class=com.pelion.connect.dm.source.PelionSourceConnector
tasks.max=2
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=io.confluent.connect.protobuf.ProtobufConverter
value.converter.schema.registry.url=http://localhost:8081

# Pelion connector specific properties
#pelion.api.host=api.us-east-1.mbedcloud.com
pelion.access.key.list=<access_key1>, <access_key2>
subscriptions=presub1, presub2, presub3, presub4, presub5
subscriptions.presub1.endpoint-name=01767982c9250000000000010011579e
subscriptions.presub1.resource-path=/3200/0/5501, /3201/0/5853
subscriptions.presub2.endpoint-type=Light
subscriptions.presub2.resource-path=sen/*
subscriptions.presub3.endpoint-type=Sensor
subscriptions.presub4.resource-path=/dev/temp, /dev/hum
subscriptions.presub5.endpoint-name=0176c7561cf3000000000001001122d4
```

#### Install Source connector
```
confluent local services connect connector load connect-pelion-source -c kafka-connect-pelion/config/source-quickstart-pelion.properties
confluent local services connect connector unload connect-pelion-source
```

### Sink Connector Example config
```
name=connect-pelion-sink

# kafka-connect-specific properties
connector.class=com.pelion.connect.dm.sink.PelionSinkConnector
tasks.max=2
topics=connect-pelion-sink-requests
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=io.confluent.connect.protobuf.ProtobufConverter
value.converter.schema.registry.url=http://localhost:8081

# Pelion connector specific properties
#pelion.api.host=api.us-east-1.mbedcloud.com
pelion.access.key=<access_key>
#max.retries=10
#retry.backoff.ms=3000
```

#### Install Sink connector
```
confluent local services connect connector load connect-pelion-sink -c kafka-connect-pelion/config/sink-quickstart-pelion.properties
confluent local services connect connector unload connect-pelion-sink
```

# Development

## Building the source

```bash
mvn clean package
```

## Contributions

Contributions are always welcome! Before you start any development please,  an issue and
start a discussion. Create a pull request against your newly created issue and we're happy to see
if we can merge your pull request. First and foremost any time you're adding code to the code base
you need to include test coverage. Make sure that you run `mvn clean package` before submitting your
pull to ensure that all of the tests, checkstyle rules, and the package can be successfully built.