# kafka-connect-pelion

![Kafka Connect Pelion](https://i.ibb.co/0rJ72Bq/kafka-connect-pelion-featured-image-github.jpg "Kafka Connect Pelion")

Kafka Connect Pelion is a [Kafka Connector](http://kafka.apache.org/documentation.html#connect) plugin which includes both  a Source and a Sink connector. The Source connector
is used to consume data from Pelion IoT platform (device registrations, observations and responses) and store
them to Apache Kafka. The Sink Connector reads messages from a Kafka topic (device management requests)
and forwards them to Pelion IoT for processing. When used in tandem, the two connectors allow communicating with
IoT devices by simply posting and reading messages to/from Kafka topics. Together with the extensive support of a number of connectors already available for external system that integrate with Apache Kafka (see [Confluent Hub](https://www.confluent.io/hub/)), the Pelion connector can be used to easily integrate in a scalable and standardized way with all those systems.

Please have a look at our [demo-example guide](./demo-example/README.md) to quickly get started by bootstrapping a local development Kafka cluster
using Docker and installing both the Pelion connector as well as Elastic and Amazon S3 connectors to forward Pelion IoT messages to those stores.
If you don't own [Pelion Ready](https://developer.pelion.com/boards/) hardware yet, you can still testbed the platform by using our [Pelion Virtual Demo](https://github.com/PelionIoT/virtual-demo-for-pelion/)
to spin-off a simulated device able to connect to Pelion IoT platform.

## Installation
Download the ZIP file and extract it into one of the directories that is listed on the Connect worker's `plugin.path` configuration properties.
This must be done on each of the installations where Connect will be run. For more information, refer to Confluent [documentation page](https://docs.confluent.io/home/connect/userguide.html#connect-installing-plugins) for installing Connector plugins.


### Source Connector Configuration
_com.pelion.connect.dm.source.PelionSourceConnector_

An example template configuration of the source connector (`source-quickstart-pelion.properties`) can be found in [the repository](https://github.com/PelionIoT/kafka-connect-pelion/blob/master/kafka-connect-pelion/config/source-quickstart-pelion.properties).
You can use it as a starting guide and make any necessary adjustments for your own environment.

The table below outlines information about each configuration setting:

| Config | Value Type | Required | Default Value | Description |
|-------------|-------------|-------------|-------------|----------------|
| tasks.max | Int | Yes | 1 | The number of tasks this connector will start. |
| key.converter | String | Yes | org.apache..StringConverter | The key converter to use when storing messages. |
| value.converter | String | Yes | io.confluent..AvroConverter | The value converter to use when storing messages. |
| pelion.api.host | String | No | api.us-east-1.mbedcloud.com | The Pelion API host. Not required unless you use an on-premise instance. |
| pelion.access.key.list | List | Yes | None | A list of [Pelion Access Keys](https://developer.pelion.com/docs/device-management/current/user-account/application-access-keys.html). The list should match the number of tasks configured since each task would be assigned an access key from the list. _NOTE: Each Access Key should belong to a different Pelion Application group._ |
| topic.prefix | String | Yes | None | The prefix to use when constructing the topic names to store Pelion messages. We follow the schema: `$topic.prefix.`{notifications,registrations,responses} |
| max.retries | Int | No | 10 | The maximum number of times to retry to connect to notification channel before failing the task. |
| retry.backoff.ms | Int | No | 60000 (1min) | The time in milliseconds to wait following a notification channel close before a retry attempt is made. |
| resource.type.mapping | List | No | Empty | A list of resource id's and their respective data types.The form should be <resource_id>:{s:string, i:integer, d:double: b:bool}. If not defined, payload would be processed as a string. |
| subscriptions| List | Yes | None | A list of custom name aliases given for each [pre-subscription](https://developer.pelion.com/docs/device-management/current/resources/resource-change-webapp.html#presubscribing-to-resources-based-on-rules) configuration. During initial connector bootstrap, the list would be equally distributed according to the provided `max.tasks`. |
| subscriptions.$alias.endpoint-name| String | Yes | None | The endpoint ID (optionally having an * character at the end) e.g: "node-001" **or** "node*". |
| subscriptions.$alias.resource-path| List | Yes | None | List of resources to pre-subscribe (optionally having an * character at the end) e.g: "/3200/0/5501, /3303/*, ..". |
| subscriptions.$alias.endpoint-type| String | Yes | None | The endpoint type e.g: "Sensor". |

> Note: For protecting the access keys and avoiding being specified in a properties file, follow the [Externalizing Secrets](https://docs.confluent.io/platform/current/connect/security.html#externalizing-secrets) guide
> provided by Confluent to securely store and utilize secrets in connector configurations.

#### Consuming Pelion messages

```
kafka-avro-console-consumer \
   --bootstrap-server localhost:9092 \
   --property schema.registry.url=http://localhost:8081 \
   --topic ${topic.prefix}.registrations
```

```
kafka-avro-console-consumer \
   --bootstrap-server localhost:9092 \
   --property schema.registry.url=http://localhost:8081 \
   --topic ${topic.prefix}.notifications
```

```
kafka-avro-console-consumer \
   --bootstrap-server localhost:9092 \
   --property schema.registry.url=http://localhost:8081 \
   --topic ${topic.prefix}.responses
```

### Sink Connector Configuration
_com.pelion.connect.dm.sink.PelionSinkConnector_

An example template configuration of the sink connector (`sink-quickstart-pelion.properties`) can be found in [the repository](https://github.com/PelionIoT/kafka-connect-pelion/blob/master/kafka-connect-pelion/config/sink-quickstart-pelion.properties).
You can use it as a starting guide and make any necessary adjustments for your own environment.

The table below outlines information about each configuration setting:

| Config | Value Type | Required | Default Value | Description |
|-------------|-------------|-------------|-------------|----------------|
| tasks.max | Int | Yes | 1 | The number of tasks this connector will start. If more that one, each task would be assigned the same Pelion access key to invoke device management requests. |
| key.converter | String | Yes | org.apache..StringConverter | The key converter to use when storing messages. |
| value.converter | String | Yes | io.confluent..AvroConverter | The value converter to use when storing messages. |
| topics | List | Yes | None | A list of topics the connector listens for device management requests. |
| pelion.api.host | String | No | api.us-east-1.mbedcloud.com | The Pelion API host. Not required unless you use an on-premise instance. |
| pelion.access.key | String | Yes | None | The [Pelion Access Key](https://developer.pelion.com/docs/device-management/current/user-account/application-access-keys.html) to invoke device management requests |
| max.retries| Int | No | 10 | The maximum number of times to retry on errors before failing the task. |
| retry.backoff.ms| Int | No | 3000 | The time in milliseconds to wait following an error before a retry attempt is made. |
| ignore.errors| Boolean | No | True | Whether the sink connector should ignore device requests response errors and continue processing (default true). |

#### Example Sending a request

1. Startup a Kafka consumer to listen for responses from Pelion Device Management:

   ```
   kafka-avro-console-consumer \
      --bootstrap-server localhost:9092 \
      --property schema.registry.url=http://localhost:8081 \
      --topic ${topic.prefix}.responses
   ```

2. Startup a producer to send a device request. The format should follow the request [avro schema](https://github.com/PelionIoT/kafka-connect-pelion/blob/master/demo-example/configs/device-request-schema.avsc):

   ```
   kafka-avro-console-producer --broker-list localhost:9092 \
   --property schema.registry.url=http://localhost:8081 --topic ${topic}.requests \
   --property value.schema="$(cat ./demo-example/configs/device-request-schema.avsc)"

   {"ep":"01795a4c18c60000000000010011c8c5","async_id":"my-async-id-get","retry":null,"expiry_seconds":null,"body": {"method":"GET","uri":"/3200/0/5501","accept":null,"content_type":null,"payload_b64":null}}
   ```

3. After a bit you should receive the following message printed in the consumer console:

   ```
   {"id":"my-async-id-get","status":200,"error":null,"payload":"300","ct":{"string":"text/plain"},"max_age":{"int":0}}
   ```

# Development

## Building the Connecto

At the root of the project issue:

```bash
cd kafka-connect-pelion
mvn clean package
```

## Enabling Debug mode

Prior to installing a Source/Sink connector configuration, you can enable DEBUG mode to trace messages from the connector tasks by issuing the following command:

```
curl -s -X PUT -H "Content-Type:application/json" http://localhost:8083/admin/loggers/com.pelion.connect.dm -d '{"level": "DEBUG"}' | jq '.'
```
