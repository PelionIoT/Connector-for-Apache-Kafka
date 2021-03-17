# QuickStart

In this tutorial we'll demonstrate the Pelion Source and Sink Kafka connectors by installing
both to a local development Kafka cluster and configured it to connect to Pelion IoT platform.
Using Kafka connect, messages coming from Pelion platform would be stored to Kafka and then been forwarded to an Elastic cluster as well as to an Amazon S3 bucket (optional) for further analysis.


In particular:

- A Pelion Source connector would be configured to consume and store messages coming from Pelion IoT
platform to Kafka.
- A Pelion Sink connector would be configured to send device requests from messages stored in a Kafka topic.

# Prerequisites

In order to follow this tutorial, you'll need:

- An account to [Pelion IoT platform](https://pelion.com/). A [free-tier access](https://os.mbed.com/pelion-free-tier/) is provided which is suffice for this tutorial. Once signed in, a [Pelion Access Key](https://developer.pelion.com/docs/device-management/current/user-account/application-access-keys.html) is required to be created.

- A working [Docker](https://www.docker.com) and [docker-compose](https://docs.docker.com/compose/) installation. 
- Either a [Pelion Ready](https://developer.pelion.com/boards/) device or our [Virtual-Demo](https://github.com/PelionIoT/virtual-demo-for-pelion) application, successfully connected to the platform. For more information about the Virtual-Demo, please check our [blog post](https://pelion.com/blog/education/try-pelion-without-any-hardware-virtual-demo/).


## Step 1 - Bootstrap a development Kafka and Elastic installation

- Clone the demo repository:
  ```
  $ git clone https://github.com/PelionIoT/kafka-connect-pelion.git
  ```

- Enter the `demo-example' folder:
  ```
  $ cd demo-example
  ```

- Start both kafka and Elastic cluster
  ```
  $ docker-compose -f docker-compose.yml -f docker-compose-elastic.yml up -d
  ```

  > NOTE: It will take some time for container images to download and start, so please be patient.

 
- Verify that all containers have started successfully: 
  ```
  $ docker-compose -f docker-compose.yml -f docker-compose-elastic.yml ps
         Name                    Command                  State                           Ports
  ----------------------------------------------------------------------------------------------------------------
  broker            /etc/confluent/docker/run        Up             0.0.0.0:9092->9092/tcp, 0.0.0.0:9101->9101/tcp
  connect           /etc/confluent/docker/run        Up (healthy)   0.0.0.0:8083->8083/tcp, 9092/tcp
  control-center    /etc/confluent/docker/run        Up             0.0.0.0:9021->9021/tcp
  elasticsearch     /bin/tini -- /usr/local/bi ...   Up (healthy)   0.0.0.0:9200->9200/tcp, 0.0.0.0:9300->9300/tcp
  kibana            /bin/tini -- /usr/local/bi ...   Up             0.0.0.0:5601->5601/tcp
  ksqldb-cli        /bin/sh                          Up
  ksqldb-server     /etc/confluent/docker/run        Up             0.0.0.0:8088->8088/tcp
  rest-proxy        /etc/confluent/docker/run        Up             0.0.0.0:8082->8082/tcp
  schema-registry   /etc/confluent/docker/run        Up             0.0.0.0:8081->8081/tcp
  zookeeper         /etc/confluent/docker/run        Up             0.0.0.0:2181->2181/tcp, 2888/tcp, 3888/tcp
  ```

- If you want to use [Pelion Virtual-Demo](https://pelion.com/blog/education/try-pelion-without-any-hardware-virtual-demo/) as a simulated device in case you don't own real hardware, you can bootstrap it now:
  ```
  docker run --name pelion-demo -p 8888:8888 -e CLOUD_SDK_API_KEY=<YOUR_PELION_API_KEY> -e SENSOR=counter pelion/virtual-demo
  ```
  
- Login to [Pelion Pelion portal](https://portal.mbedcloud.com) and make note of the connected Device ID:
  
  ![Pelion Portal](https://i.ibb.co/wzbfz1b/portal-device-id.png "Pelion Portal")


## Step 2 - Adjust Connector Configurations:

- Prior to deploying the Pelion connectors we need to adjust their configuration. First, let's start with the Source connector.
  Open the `configs/source-quickstart-pelion` configuration file and adjust the following entries with your Pelion access key and
  device id:
  ```
  ...
  "pelion.access.key.list": "<PELION_ACCESS_KEY>"
  "subscriptions.presub1.endpoint-name": "<DEVICE_ID>"
  ...
  ```
- Similar for the Sink connector, open the `configs/sink-quickstart-pelion.json` file:
  ```
  "pelion.access.key": "<PELION_ACCESS_KEY>"
  ```

## Step 3 - Deploy Connector configurations:

### Source Connector

- Using `curl`, post the source connector configuration:
  ```
  $ curl -s -H "Content-Type: application/json" -X POST -d @"./configs/source-quickstart-pelion.json" http://localhost:8083/connectors/ | jq .
  ```
  If successfully, you should see the configuration printed out in the console and the connector starting receiving messages from Pelion:

- Start a kafka consumer to verify that messages are being received:
  ```
  $ kafka-protobuf-console-consumer \
   --bootstrap-server localhost:9092 \
   --property schema.registry.url=http://localhost:8081 \
   --topic mypelion.notifications
  ```

### Sink Connector

- Using `curl`, post the sink connector configuration:
  ```
  $ curl -s -H "Content-Type: application/json" -X POST -d @"./configs/sink-quickstart-pelion.json" http://localhost:8083/connectors/ | jq .
  ```
  If successfully, you should see the configuration printed out in the console and the connector waiting for device requests to be posted in `mypelion.requests` topic.

- Start a kafka consumer to listen for responses:
  ```
  kafka-protobuf-console-consumer \
     --bootstrap-server localhost:9092 \
     --property schema.registry.url=http://localhost:8081 \
     --topic mypelion.responses
  ```
  
  Start a Kafka producer to post device request messages:
  ```
  kafka-protobuf-console-producer --broker-list localhost:9092 \
     --property schema.registry.url=http://localhost:8081 --topic mypelion.requests \
     --property value.schema='syntax = "proto3"; message DeviceRequest {string ep = 1; string async_id = 2; int32 retry = 3; int64 expiry_seconds = 4; message Body {enum Method {GET = 0; PUT = 1; POST = 2; DELETE = 3;} Method method = 1; string uri = 2; string accept = 3; string content_type = 4; string payload_b64 = 5;} Body body = 5;}'
  ```
  
  Once started, enter the following request. Replace the `<device_id>` with your own device id:
  ```
  {"ep": "<device_id>", "async_id":"my-req-id", "body": {"method": "GET", "uri": "/3200/0/5501"}}
  ```

  If successfully, you should see the following printed in the consumer console:
  ```
  {"id":"my-req-id","status":"SUCCESS","error":"","payload":"1166","ct":"text/plain","maxAge":0}
  ```


## Step 4 - Deploy Elastic connector configuration

- Prior to deploying Elastic connector, we need to configure the mapping for the `timestamp` field so it's properly indexed:
  ```
  $ curl -X PUT http://localhost:9200/_template/kafka-connect \
        -H "Content-Type:application/json" \
        -d '{
      "index_patterns": ["*"],
      "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0
      },
      "mappings": {
        "_source": { "enabled": true },
        "dynamic_templates": [
          {
            "longs_as_dates": {
              "match_mapping_type": "long",
              "match" : "timestamp",
              "mapping": { "type": "date" }
            }
          }
        ]
      }
    }'
   ```
- We are now ready to deploy Elastic connector:
  ```
  $ curl -s -H "Content-Type: application/json" -X POST -d @"./configs/sink-elastic.json" http://localhost:8083/connectors/ | jq .
  ```
  The connector is configured to consume messages from the `mypelion.notifications` and `mypelion.registrations` topics and forward all messages to Elastic

  If you open [Elastic console](http://localhost:5601) and after configuring the necessary index patterns you should see the messages flowing in:

  ![Elastic](https://i.ibb.co/kcZx75m/elastic.png "Elastic")


## Step 5 - (Optional) Deploy Amazon S3 connector configuration

- If you are an Amazon S3 user, we include a `configs/sink-amazon-s3.json` configuration to also forward messages to S3 for storage.
  Adjust the configuration with your own S3 bucket name and region and deploy the connector:

  ```
  $ curl -s -H "Content-Type: application/json" -X POST -d @"./configs/sink-amazon-s3.json" http://localhost:8083/connectors/ | jq .
  ```
  
  > Check Confluent's [Amazon S3 Sink Connector](https://docs.confluent.io/kafka-connect-s3-sink/current/index.html) configuration for more information:

  ![Amazon S3](https://i.ibb.co/7j4Bhvf/amazon-s3.png "Amazon S3")
  
  