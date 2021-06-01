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
- A working [Docker](https://www.docker.com) and [docker-compose](https://docs.docker.com/compose/) installation. If you are on a Mac or Windows, please increase the memory size allocated to Docker, 6Gb should be enough.
- Either a [Pelion Ready](https://developer.pelion.com/boards/) device or our [Virtual-Demo](https://github.com/PelionIoT/virtual-demo-for-pelion) application, successfully connected to the platform. For more information about the Virtual-Demo, please check our [blog post](https://pelion.com/blog/education/try-pelion-without-any-hardware-virtual-demo/).
- [curl](https://curl.se/) and [jq](https://stedolan.github.io/jq/) tools.

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

- If you want to use [Pelion Virtual-Demo](https://pelion.com/blog/education/try-pelion-without-any-hardware-virtual-demo/) as a simulated device in case you don't
  own real hardware, you can bootstrap it now. Replace `YOUR_PELION_API_KEY` with your own key:
  ```
  docker run --name pelion-demo -p 8888:8888 -e SENSOR=counter -e CLOUD_SDK_API_KEY=<YOUR_PELION_API_KEY> pelion/virtual-demo
  ```
  
  Open the [virtual demo page](http://localhost:8888/) in a new browser tab:

  ![Pelion Virtual Demo](https://i.ibb.co/5TXDv6n/pelion-virtual-demo.png "Pelion Virtual Demo")
  
- Login to [Pelion Pelion portal](https://portal.mbedcloud.com) and make note of the connected Device ID:
  
  ![Pelion Portal](https://i.ibb.co/0XTJYzS/portal-device-id.png "Pelion Portal")


## Step 2 - Adjust Connector Configurations:

- Prior to deploying the Pelion connectors we need to adjust their configuration. First, let's start with the Source connector.
  Open the `configs/source-quickstart-pelion` configuration file and adjust the following entries with your Pelion access key and
  device id:
  
    > NOTE: For protecting the access keys and avoiding being specified in a properties file in a production environment, follow the [Externalizing Secrets](https://docs.confluent.io/platform/current/connect/security.html#externalizing-secrets) guide provided by Confluent to securely store and utilize secrets in connector configurations.

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

Prior to deployment, we recommend starting a logs session for the connect cluster container in order to track any errors during deployment of the connector configurations. On a new terminal, simply enter:
```
docker logs -f connect
```

### Source Connector

- Using `curl`, post the source connector configuration:
  ```
  $ curl -s -H "Content-Type: application/json" -X POST -d @"./configs/source-quickstart-pelion.json" http://localhost:8083/connectors/ | jq .
  ```
  If successfully, you should see the configuration printed out in the console and the connector starting receiving messages from Pelion:

  > NOTE: As described in our [pre-subscriptions documentation page](https://developer.pelion.com/docs/device-management/current/resources/resource-change-webapp.html#presubscribing-to-resources-based-on-rules), Pelion 
  > Device Management does not deploy pre-subscription changes to the device immediately. Instead, when the device update it's registration status to the platform, then the rules are forwarded. For that reason, please ensure
  > you reboot the device or in case of the Virtual demo, a simple `docker stop/start` on the virtual demo container should suffice. Once your device reboots and registers again to the platform, you should start receiving messages.
- Start kafka consumers to verify that Pelion device messages are being received and stored by the Source Connector:


  ***Device Registrations:***
  ```
  $ docker exec -it connect kafka-avro-console-consumer \
   --bootstrap-server broker:29092 \
   --property schema.registry.url=http://schema-registry:8081 \
   --topic mypelion.registrations
  ```
  > (NOTE: You may need to reboot the device or start/stop the virtual demo for the registrations to appear)

  ***Device Observations:***
  ```
  $ docker exec -it connect kafka-avro-console-consumer \
   --bootstrap-server broker:29092 \
   --property schema.registry.url=http://schema-registry:8081 \
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
  docker exec -it connect kafka-avro-console-consumer \
   --bootstrap-server broker:29092 \
   --property schema.registry.url=http://schema-registry:8081 \
   --topic mypelion.responses
  ```
  
  Start a Kafka producer to post device request messages:
  ```
  docker exec -it connect kafka-avro-console-producer --broker-list broker:29092 \
     --property schema.registry.url=http://schema-registry:8081 --topic mypelion.requests \
     --property value.schema="$(cat ./configs/device-request-schema.avsc)"
  ```
  
  Once started, enter the following request to retrieve the current value of the '/3200/0/5501' resource. Replace the `<device_id>` with your own device id:
  ```
  {"ep":"<device_id>","async_id":"my-async-id-get","retry":null,"expiry_seconds":null,"body": {"method":"GET","uri":"/3200/0/5501","accept":null,"content_type":null,"payload_b64":null}}
  ```

  If successfully, you should see the following printed in the consumer console:
  ```
  {"id":"my-async-id-get","status":200,"error":null,"payload":"300","ct":{"string":"text/plain"},"max_age":{"int":0}}
    ```
  
  You can also try to blink the LED on the device by sending the following message:

  ```
  {"ep":"<device_id>","async_id":"my-async-id-post","retry":null,"expiry_seconds":null,"body": {"method":"POST","uri":"/3201/0/5850","accept":null,"content_type":null,"payload_b64":null}}
  ```


## Step 4 - Deploy Elastic connector configuration

- Prior to deploying Elastic connector, we need to configure the mapping in Elastic for the `timestamp` field, so it's properly indexed:
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

  If you open [Elastic console](http://localhost:5601) and after [configuring the necessary index patterns](https://www.elastic.co/guide/en/kibana/current/index-patterns.html) you should see the messages flowing in:

  ![Elastic](https://i.ibb.co/4P3cy07/elastic.png "Elastic")


## Step 5 - (Optional) Deploy Amazon S3 connector configuration

- If you are an Amazon S3 user, we include a `configs/sink-amazon-s3.json` configuration to also forward messages to S3 for storage.
  Prior to deploying it, you first need to perform the following:
    - Uncomment `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` in `docker-compose.yml` file, passing your own AWS credentials.
    - Stop and Restart Connect cluster for the aws credentials to take effect
        ```
        docker-compose rm -svf connect
        docker-compose -f docker-compose.yml -f docker-compose-elastic.yml up -d connect
        ```
    - Adjust connector configuration with your own S3 bucket name (`s3.bucket.name`) and region (`s3.region`)
    - Once completed, you are ready to deploy it:
      ```
      $ curl -s -H "Content-Type: application/json" -X POST -d @"./configs/sink-amazon-s3.json" http://localhost:8083/connectors/ | jq .
      ```
    
  > Check Confluent's [Amazon S3 Sink Connector](https://docs.confluent.io/kafka-connect-s3-sink/current/index.html) configuration for more information regarding configuration parameters:

  ![Amazon S3](https://i.ibb.co/nwqntG1/amazon-s3.png "Amazon S3")

## Step 6 - Streaming Analytics with [ksqldb](https://ksqldb.io)

- Now that Pelion device messages are stored in Kafka, we can start a ksqldb session to perform real-time streaming analytics:
  ```
  docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
  ```
  
  You should be prompted with the ksqlDB prompt:
  ```
  
              ===========================================
              =       _              _ ____  ____       =
              =      | | _____  __ _| |  _ \| __ )      =
              =      | |/ / __|/ _` | | | | |  _ \      =
              =      |   <\__ \ (_| | | |_| | |_) |     =
              =      |_|\_\___/\__, |_|____/|____/      =
              =                   |_|                   =
              =  Event Streaming Database purpose-built =
              =        for stream processing apps       =
              ===========================================

  Copyright 2017-2020 Confluent Inc.
  
  CLI v6.1.0, Server v6.1.0 located at http://ksqldb-server:8088
  Server Status: RUNNING
  
  Having trouble? Type 'help' (case-insensitive) for a rundown of how things work!
  
  ksql>
  ```
  
- Enter the following query to create a new stream bound to the `notifications` topic:
  ```
  ksql> CREATE STREAM "mypelion-notifications"
  WITH (
       KAFKA_TOPIC='mypelion.notifications',
       VALUE_FORMAT='AVRO'
  );
  ```

- If you query the stream now, real-time notifications from the device should start be printed out:   
  ```
  ksql> SELECT * FROM "mypelion-notifications" EMIT CHANGES;
  +------------+------------+------------+------------+------------+------------+------------+------------+------------+
  |EP          |PATH        |CT          |PAYLOAD_B64 |PAYLOAD     |MAX_AGE     |UID         |TIMESTAMP   |ORIGINAL_EP |
  +------------+------------+------------+------------+------------+------------+------------+------------+------------+
  |01795a4c18c6|/3200/0/5501|text/plain  |MTE2        |{S=null, L=1|0           |317068f4-a8d|162074241625|01795a4c18c6|
  |000000000001|            |            |            |16, D=null, |            |b-459c-84fc-|1           |000000000001|
  |0011c8c5    |            |            |            |B=null}     |            |58edc80d6d34|            |0011c8c5    |
  |01795a4c18c6|/3200/0/5501|text/plain  |MTE3        |{S=null, L=1|0           |cda8b07b-372|162074242126|01795a4c18c6|
  |000000000001|            |            |            |17, D=null, |            |3-446b-8bf7-|2           |000000000001|
  |0011c8c5    |            |            |            |B=null}     |            |de2981683082|            |0011c8c5    |
  |01795a4c18c6|/3200/0/5501|text/plain  |MTE4        |{S=null, L=1|0           |4c2e443f-da9|162074242627|01795a4c18c6|
  |000000000001|            |            |            |18, D=null, |            |3-47fe-9bf5-|2           |000000000001|
  |0011c8c5    |            |            |            |B=null}     |            |6e4bc9f53a31|            |0011c8c5    |
  |01795a4c18c6|/3200/0/5501|text/plain  |MTE5        |{S=null, L=1|0           |859d0902-7f0|162074243128|01795a4c18c6|
  |000000000001|            |            |            |19, D=null, |            |6-4bd1-be43-|0           |000000000001|
  |0011c8c5    |            |            |            |B=null}     |            |fe6d071af03d|            |0011c8c5    |
  ```

- Let's create a sample analytic job to count the notifications received from an endpoint and resource path over a window of 10 secs
  and if this count exceeds 3 then let's write it to a different topic ***'alerts'***:
  ```
  ksql> CREATE TABLE alerts
        WITH (
        kafka_topic = 'alerts',
        value_format = 'AVRO'
        ) AS SELECT
        ep,path,
        AS_VALUE(ep) AS "endpoint",
        AS_VALUE(path) AS "path",
        COUNT(*) AS "readings" FROM "mypelion-notifications"
        WINDOW TUMBLING (SIZE 10 SECONDS)
        GROUP BY ep, path HAVING COUNT(*) > 3
        EMIT CHANGES;
  ```
- Start a consumer on the 'alerts' topic:
  ```
  docker exec -it connect kafka-avro-console-consumer \
   --bootstrap-server broker:29092 \
   --property schema.registry.url=http://schema-registry:8081 \
   --topic alerts
  ```
- Continue to click the button on the device (or in the virtual demo) and see alert messages being printed out:
  ```
  {"endpoint":{"string":"01795a4c18c60000000000010011c8c5"},"path":{"string":"/3200/0/5501"},"readings":{"long":4}}
  {"endpoint":{"string":"01795a4c18c60000000000010011c8c5"},"path":{"string":"/3200/0/5501"},"readings":{"long":5}}
  ```
  
- As an exercise, you can deploy another connector to forward those `alert` messages to a Slack channel for instant notifications!
  We have included [Confluent's Http Sink connector](https://www.confluent.io/hub/confluentinc/kafka-connect-http) which together with [Zapier service](https://zapier.com),
  you can set up a real-time notification pipeline. 
  
  Setup a webhook in Zapier by following the [documentation](https://zapier.com/help/create/code-webhooks/send-webhooks-in-zaps) and once done replace the `http.api.url`
  in the sink configuration and deploy:
  
  ```
  curl -s -H "Content-Type: application/json" -X POST -d @"./configs/sink-http-webook-zapier.json" http://localhost:8083/connectors/ | jq .
  ```
  
- Generate again alert messages by continuously clicking the button on the device (or in the virtual demo) and if everything is configured correctly,
  alert messages should appear in Slack!

  ![Slack](https://i.ibb.co/xM0stt0/slack.png "Slack")
  
