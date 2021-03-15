
Bootstrap all containers:
```
➜ docker-compose -f docker-compose.yml -f docker-compose-elastic.yml up -d
```

> NOTE: It will take some time for all containers to start, please be patient.

 
Verify that all containers have started successfully: 
```
➜ docker-compose -f docker-compose.yml -f docker-compose-elastic.yml ps
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

Deploy Connector Configuration:


Pelion source:
```
➜ curl -s -H "Content-Type: application/json" -X POST -d @"./configs/source-quickstart-pelion.json" http://localhost:8083/connectors/ | jq .
```

Pelion sink:
```
➜ curl -s -H "Content-Type: application/json" -X POST -d @"./configs/sink-quickstart-pelion.json" http://localhost:8083/connectors/ | jq .
```

Prior to deploying Elastic connector, we need to configure mapping for the 'timestamp' field:

```
➜ curl -X PUT http://localhost:9200/_template/kafka-connect \
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


We are now ready to deploy Elastic connector:
```
curl -s -H "Content-Type: application/json" -X POST -d @"./configs/sink-elastic.json" http://localhost:8083/connectors/ | jq .
```
