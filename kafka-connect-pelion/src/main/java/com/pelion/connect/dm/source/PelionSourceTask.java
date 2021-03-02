/*
 * Copyright 2021 Pelion Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pelion.connect.dm.source;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pelion.connect.dm.utils.PelionAPI;
import com.pelion.protobuf.PelionProtos.AsyncIDResponse;
import com.pelion.protobuf.PelionProtos.NotificationData;
import io.confluent.connect.protobuf.ProtobufData;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.http.WebSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;

import static com.pelion.connect.dm.utils.PelionConnectorUtils.base64Decode;
import static com.pelion.connect.dm.utils.PelionConnectorUtils.getVersion;
import static com.pelion.connect.dm.utils.PelionConnectorUtils.readFile;
import static com.pelion.protobuf.PelionProtos.EndpointData;
import static com.pelion.protobuf.PelionProtos.ResourceData;

public class PelionSourceTask extends SourceTask {

  private static final Logger LOG = LoggerFactory.getLogger(PelionSourceTask.class);

  private PelionSourceTaskConfig config;

  private BlockingQueue<String> queue;

  private static final ObjectMapper mapper = new ObjectMapper();
  private static final ProtobufData protobufData = new ProtobufData();

  private PelionAPI pelionAPI;
  private WebSocket ws;

  private String ndTopic;
  private String edTopic;
  private String arTopic;

  private boolean shouldPublishRegistrations;

  @Override
  public String version() {
    return getVersion();
  }

  public void start(Map<String, String> props) {
    start(props, null, null);
  }

  // visible for testing
  public void start(Map<String, String> props, PelionAPI api, BlockingQueue<String> q) {
    LOG.info(readFile("pelion-source-ascii.txt"));

    // initialize config
    this.config = new PelionSourceTaskConfig(props);
    // the kafka topics where we store records
    // follows the schema: {connector-name}-{notifications|registrations}
    this.ndTopic = String.format("%s-notifications",
        props.get(PelionSourceConnectorConfig.CONNECT_NAME_CONFIG));
    this.edTopic = String.format("%s-registrations",
        props.get(PelionSourceConnectorConfig.CONNECT_NAME_CONFIG));
    this.arTopic = String.format("%s-async-responses",
        props.get(PelionSourceConnectorConfig.CONNECT_NAME_CONFIG));
    // whether this task should publish registrations events
    this.shouldPublishRegistrations = config.getBoolean(PelionSourceTaskConfig.PELION_TASK_PUBLISH_REGISTRATIONS);

    this.queue = q != null ? q : new LinkedBlockingQueue<>();
    this.pelionAPI = api != null ? api :
        new PelionAPI(config.getString(PelionSourceConnectorConfig.PELION_API_HOST_CONFIG),
            config.getPassword(PelionSourceTaskConfig.PELION_TASK_ACCESS_KEY_CONFIG).value());

    // setup pre-subscriptions
    pelionAPI.createPreSubscriptions(config);
    // connect ws notification channel
    ws = pelionAPI.connectNotificationChannel(new PelionSourceTask.WebSocketListener());
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    final List<SourceRecord> records = new ArrayList<>();

    try {
      final String message = queue.take(); // block until msg is received
      // parse json incoming message
      final JsonNode jsonBody = mapper.readTree(message);

      // determine message type
      if (jsonBody.has("notifications")) {
        final JsonNode jsonData = jsonBody.get("notifications");
        for (final JsonNode jsonNode : jsonData) {
          // build protobuf obj from json
          final NotificationData nd = buildNotificationData(jsonNode);
          // build connect schema/value from protobuf
          final SchemaAndValue schemaAndValue = protobufData
              .toConnectData(Schemas.PROTOBUF_ND_SCHEMA, nd);
          // build the connect record
          SourceRecord record = new SourceRecord(
              null,
              null,
              this.ndTopic,
              null,
              SchemaBuilder.string().build(),
              nd.getEp(),
              schemaAndValue.schema(),
              schemaAndValue.value());

          // add it to our list
          records.add(record);
        }

      } else if (jsonBody.has("registrations")) {
        if (!shouldPublishRegistrations) {
          LOG.debug("[{}] skipping registration incoming message [shouldPublishRegistration=false]", Thread.currentThread().getName());
        } else {
          final JsonNode jsonData = jsonBody.get("registrations");
          for (final JsonNode jsonNode : jsonData) {
            // build protobuf obj from json
            final EndpointData ed = buildEndpointData(jsonNode);
            // build the connect record
            final SchemaAndValue schemaAndValue = protobufData
                .toConnectData(Schemas.PROTOBUF_ED_SCHEMA, ed);
            // build the connect record
            SourceRecord record = new SourceRecord(
                null,
                null,
                this.edTopic,
                null,
                SchemaBuilder.string().build(),
                ed.getEp(),
                schemaAndValue.schema(),
                schemaAndValue.value());

            // add it to our list
            records.add(record);
          }
        }
      } else if (jsonBody.has("async-responses")) {
        final JsonNode jsonData = jsonBody.get("async-responses");
        for (final JsonNode jsonNode : jsonData) {
          // build protobuf obj from json
          final AsyncIDResponse ar = buildAsyncIDResponse(jsonNode);
          // build the connect record
          final SchemaAndValue schemaAndValue = protobufData
              .toConnectData(Schemas.PROTOBUF_AR_SCHEMA, ar);
          // build the connect record
          SourceRecord record = new SourceRecord(
              null,
              null,
              this.arTopic,
              null,
              SchemaBuilder.string().build(),
              ar.getId(),
              schemaAndValue.schema(),
              schemaAndValue.value());

          // add it to our list
          records.add(record);
        }

      } else {
        LOG.debug("[{}] skipping unknown incoming message", Thread.currentThread().getName());
      }
    } catch (JsonProcessingException e) {
      e.printStackTrace();
    }

    LOG.debug("[{}] returning {} records ", Thread.currentThread().getName(), records.size());
    return records;
  }

  public NotificationData buildNotificationData(JsonNode jsonNode) {
    NotificationData.Builder notification = NotificationData.newBuilder();

    notification.setEp(jsonNode.get("ep").asText());
    notification.setPath(jsonNode.get("path").asText());
    notification.setCt(jsonNode.get("ct").asText());
    notification.setPayload(base64Decode(jsonNode.get("payload").asText()));
    notification.setMaxAge(jsonNode.get("max-age").asInt());
    notification.setUid(jsonNode.get("uid").asText());
    notification.setTimestamp(jsonNode.get("timestamp").asLong());
    notification.setOriginalEp(jsonNode.get("original-ep").asText());

    return notification.build();
  }

  private EndpointData buildEndpointData(JsonNode jsonNode) {
    EndpointData.Builder endpoint = EndpointData.newBuilder();

    endpoint.setEp(jsonNode.get("ep").asText());
    endpoint.setOriginalEp(jsonNode.get("original-ep").asText());
    endpoint.setEpt(jsonNode.get("ept").asText());
    // note: we use 'path()' instead of 'get()' for optional fields
    endpoint.setQ(jsonNode.path("q").asBoolean());
    endpoint.setTimestamp(jsonNode.get("timestamp").asLong());

    final JsonNode jsonData = jsonNode.get("resources");
    for (final JsonNode r : jsonData) {
      ResourceData.Builder resource = ResourceData.newBuilder();
      resource.setPath(r.get("path").asText());
      resource.setIf(r.path("if").asText());
      resource.setRt(r.path("rt").asText());
      resource.setCt(r.path("ct").asText());
      resource.setObs(r.path("obs").asBoolean());

      endpoint.addResource(resource.build());
    }

    return endpoint.build();
  }

  public AsyncIDResponse buildAsyncIDResponse(JsonNode jsonNode) {
    AsyncIDResponse.Builder response = AsyncIDResponse.newBuilder();

    response.setId(jsonNode.get("id").asText());
    response.setPayload(base64Decode(jsonNode.get("payload").asText()));
    response.setStatus(AsyncIDResponse.Status.forNumber(jsonNode.get("status").asInt()));
    response.setError(jsonNode.path("error").asText());
    response.setCt(jsonNode.path("ct").asText());
    response.setMaxAge(jsonNode.path("max-age").asInt());

    return response.build();
  }

  @Override
  public void stop() {
    LOG.info("Stopping PelionSourceTask");
    if (ws != null) {
      ws.sendClose(WebSocket.NORMAL_CLOSURE, "closing");
    }
  }

  static class Schemas {

    // "notification" protobuf schema
    static final ProtobufSchema PROTOBUF_ND_SCHEMA = new ProtobufSchema(
        NotificationData.getDescriptor());

    // "registration" protobuf schema
    static final ProtobufSchema PROTOBUF_ED_SCHEMA = new ProtobufSchema(
        EndpointData.getDescriptor());

    // "async-response" protobuf schema
    static final ProtobufSchema PROTOBUF_AR_SCHEMA = new ProtobufSchema(
        AsyncIDResponse.getDescriptor());
  }

  class WebSocketListener implements WebSocket.Listener {

    StringBuilder text = new StringBuilder();

    @Override
    public void onOpen(WebSocket webSocket) {
      LOG.debug("[{}] onOpen()", Thread.currentThread().getName());
      WebSocket.Listener.super.onOpen(webSocket);
    }

    @Override
    public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
      text.append(data);
      if (last) {
        queue.add(text.toString());
        LOG.debug("[{}] {}", Thread.currentThread().getName(), text);
        text = new StringBuilder();
      }
      return WebSocket.Listener.super.onText(webSocket, data, last);
    }

    @Override
    public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
      LOG.debug("[{}] onClose()", Thread.currentThread().getName());
      return WebSocket.Listener.super.onClose(webSocket, statusCode, reason);
    }

    @Override
    public void onError(WebSocket webSocket, Throwable error) {
      LOG.debug("[{}] onError()", Thread.currentThread().getName(), error);
      WebSocket.Listener.super.onError(webSocket, error);
    }
  }
}