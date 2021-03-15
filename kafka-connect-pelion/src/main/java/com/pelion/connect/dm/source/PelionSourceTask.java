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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pelion.connect.dm.utils.PelionAPI;
import com.pelion.protobuf.PelionProtos.AsyncIDResponse;
import com.pelion.protobuf.PelionProtos.EndpointData;
import com.pelion.protobuf.PelionProtos.NotificationData;
import com.pelion.protobuf.PelionProtos.ResourceData;
import io.confluent.connect.protobuf.ProtobufData;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.http.WebSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.pelion.connect.dm.utils.PelionConnectorUtils.BOOLEAN;
import static com.pelion.connect.dm.utils.PelionConnectorUtils.DOUBLE;
import static com.pelion.connect.dm.utils.PelionConnectorUtils.INTEGER;
import static com.pelion.connect.dm.utils.PelionConnectorUtils.base64Decode;
import static com.pelion.connect.dm.utils.PelionConnectorUtils.getVersion;
import static com.pelion.connect.dm.utils.PelionConnectorUtils.readFile;
import static com.pelion.connect.dm.utils.PelionConnectorUtils.sleep;
import static com.pelion.connect.dm.utils.PelionConnectorUtils.uniqueIndex;

public class PelionSourceTask extends SourceTask {

  private static final Logger LOG = LoggerFactory.getLogger(PelionSourceTask.class);

  private PelionSourceTaskConfig config;

  private BlockingQueue<String> queue;

  private static final ObjectMapper mapper = new ObjectMapper();
  private static final ProtobufData protobufData = new ProtobufData();

  private PelionAPI pelionAPI;

  private String ndTopic;
  private String edTopic;
  private String arTopic;

  private boolean shouldPublishRegistrations;

  private Map<String, List<String>> types;

  private WebSocket websocket;
  private PelionSourceTask.WebSocketListener wsListener;

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
    // follows the schema: {TOPIC_PREFIX}-{notifications|registrations|async-responses}
    this.ndTopic = String.format("%s.notifications",
        props.get(PelionSourceConnectorConfig.TOPIC_PREFIX));
    this.edTopic = String.format("%s.registrations",
        props.get(PelionSourceConnectorConfig.TOPIC_PREFIX));
    this.arTopic = String.format("%s.responses",
        props.get(PelionSourceConnectorConfig.TOPIC_PREFIX));
    // whether this task should publish registrations events
    this.shouldPublishRegistrations = config.getBoolean(PelionSourceTaskConfig.PELION_TASK_PUBLISH_REGISTRATIONS);
    // configured type mappings
    this.types = uniqueIndex(config.getList(PelionSourceConnectorConfig.RESOURCE_TYPE_MAPPING_CONFIG));

    this.queue = q != null ? q : new LinkedBlockingQueue<>();
    this.pelionAPI = api != null ? api :
        new PelionAPI(config.getString(PelionSourceConnectorConfig.PELION_API_HOST_CONFIG),
            config.getPassword(PelionSourceTaskConfig.PELION_TASK_ACCESS_KEY_CONFIG).value());

    // setup pre-subscriptions
    pelionAPI.createPreSubscriptions(config);
    // connect websocket channel
    connectWebSocketChannel();

    LOG.info("[{}] Started Pelion source task", Thread.currentThread().getName());
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    // check whether reconnection is required
    checkIfReconnectWsNeeded();

    // now block until message is received or timeout on poll occurs. We
    // need to return regularly (with null) to allow task transitions (e.g PAUSE)
    final String message = queue.poll(10, TimeUnit.SECONDS);
    // no message received yet, return
    if (message == null) {
      return null;
    }

    // ok, let's process
    final List<SourceRecord> records = new ArrayList<>();

    try {
      // parse message
      final JsonNode jsonBody = mapper.readTree(message);
      // determine type
      if (jsonBody.has("notifications")) {
        processNotificationData(records, jsonBody.get("notifications"));
      } else if (jsonBody.has("registrations")) {
        processEndpointData(records, jsonBody.get("registrations"));
      } else if (jsonBody.has("async-responses")) {
        processAsyncResponses(records, jsonBody.get("async-responses"));
      }

      LOG.debug("[{}] returning {} record ", Thread.currentThread().getName(), records.size());
      return records;

    } catch (IOException e) {
      closeResources();
      throw new ConnectException(e);
    }
  }

  @Override
  public void stop() {
    LOG.info("[{}] Stopping Pelion source task", Thread.currentThread().getName());
    closeResources();
  }

  private void processNotificationData(List<SourceRecord> records, JsonNode jsonData) {
    for (final JsonNode jsonNode : jsonData) {
      // build protobuf obj from json
      NotificationData.Builder notification = NotificationData.newBuilder();

      notification.setEp(jsonNode.get("ep").asText());
      notification.setPath(jsonNode.get("path").asText());
      notification.setCt(jsonNode.get("ct").asText());
      notification.setPayloadB64(jsonNode.get("payload").asText());
      notification.setMaxAge(jsonNode.get("max-age").asInt());
      notification.setUid(jsonNode.get("uid").asText());
      notification.setTimestamp(jsonNode.get("timestamp").asLong());
      notification.setOriginalEp(jsonNode.get("original-ep").asText());

      // determine resource
      String resource = notification.getPath().split("/")[3];
      // check the type associated with the resource
      if (mappingExists(INTEGER, resource)) {
        notification.setL(Long.parseLong(base64Decode(notification.getPayloadB64())));
      } else if (mappingExists(DOUBLE, resource)) {
        notification.setD(Double.parseDouble(base64Decode(notification.getPayloadB64())));
      } else if (mappingExists(BOOLEAN, resource)) {
        notification.setB(Boolean.parseBoolean(base64Decode(notification.getPayloadB64())));
      } else { // treat it as a generic string
        notification.setS(base64Decode(notification.getPayloadB64()));
      }

      // build connect schema/value from protobuf
      final SchemaAndValue schemaAndValue = protobufData
          .toConnectData(Schemas.PROTOBUF_ND_SCHEMA, notification.build());
      // build the connect record
      SourceRecord record = new SourceRecord(
          null,
          null,
          this.ndTopic,
          null,
          SchemaBuilder.string().build(),
          notification.getEp(),
          schemaAndValue.schema(),
          schemaAndValue.value());

      records.add(record);
    }
  }

  private void processEndpointData(List<SourceRecord> records, JsonNode jsonData) {
    if (!shouldPublishRegistrations) {
      LOG.debug("[{}] skipping registration incoming message [shouldPublishRegistration=false]", Thread.currentThread().getName());
      return;
    }

    for (final JsonNode jsonNode : jsonData) {
      EndpointData.Builder endpoint = EndpointData.newBuilder();

      endpoint.setEp(jsonNode.get("ep").asText());
      endpoint.setOriginalEp(jsonNode.get("original-ep").asText());
      endpoint.setEpt(jsonNode.get("ept").asText());
      // note: we use 'path()' instead of 'get()' for optional fields
      endpoint.setQ(jsonNode.path("q").asBoolean());
      endpoint.setTimestamp(jsonNode.get("timestamp").asLong());

      final JsonNode resources = jsonNode.get("resources");
      for (final JsonNode r : resources) {
        ResourceData.Builder resource = ResourceData.newBuilder();
        resource.setPath(r.get("path").asText());
        resource.setIf(r.path("if").asText());
        resource.setRt(r.path("rt").asText());
        resource.setCt(r.path("ct").asText());
        resource.setObs(r.path("obs").asBoolean());

        endpoint.addResource(resource.build());
      }

      // build the connect record
      final SchemaAndValue schemaAndValue = protobufData
          .toConnectData(Schemas.PROTOBUF_ED_SCHEMA, endpoint.build());
      // build the connect record
      SourceRecord record = new SourceRecord(
          null,
          null,
          this.edTopic,
          null,
          SchemaBuilder.string().build(),
          endpoint.getEp(),
          schemaAndValue.schema(),
          schemaAndValue.value());

      // add it to our list
      records.add(record);
    }
  }

  private void processAsyncResponses(List<SourceRecord> records, JsonNode jsonData) {
    for (final JsonNode jsonNode : jsonData) {
      // build protobuf obj from json
      AsyncIDResponse.Builder response = AsyncIDResponse.newBuilder();

      response.setId(jsonNode.get("id").asText());
      response.setPayload(base64Decode(jsonNode.get("payload").asText()));
      response.setStatus(AsyncIDResponse.Status.forNumber(jsonNode.get("status").asInt()));
      response.setError(jsonNode.path("error").asText());
      response.setCt(jsonNode.path("ct").asText());
      response.setMaxAge(jsonNode.path("max-age").asInt());

      // build the connect record
      final SchemaAndValue schemaAndValue = protobufData
          .toConnectData(Schemas.PROTOBUF_AR_SCHEMA, response.build());
      // build the connect record
      SourceRecord record = new SourceRecord(
          null,
          null,
          this.arTopic,
          null,
          SchemaBuilder.string().build(),
          response.getId(),
          schemaAndValue.schema(),
          schemaAndValue.value());

      // add it to our list
      records.add(record);
    }
  }

  private boolean mappingExists(String type, String value) {
    return types.containsKey(type) && types.get(type).contains(value);
  }

  private void closeResources() {
    if (websocket != null) {
      websocket.sendClose(WebSocket.NORMAL_CLOSURE, "closing");
    }
  }

  private void checkIfReconnectWsNeeded() {
    if (websocket != null && websocket.isInputClosed()) {
      LOG.info("[{}] Websocket closed! code:'{}', reason:'{}'", Thread.currentThread().getName(),
          wsListener.wsSocketStatus, wsListener.wsCloseReason);
      if (canReconnect()) {
        LOG.info("[{}] Sleeping and attempting to reconnect..", Thread.currentThread().getName());
        sleep(1000 * 60); // wait 1min prior reconnecting
        connectWebSocketChannel();
      } else {
        throw new ConnectException(String.format("Websocket terminated! unable to auto-reconnect due to received "
            + "socket status, code:'%d', reason:'%s'", wsListener.wsSocketStatus, wsListener.wsCloseReason));
      }
    }
  }

  private void connectWebSocketChannel() {
    wsListener = new PelionSourceTask.WebSocketListener();
    websocket = pelionAPI.connectNotificationChannel(wsListener);
  }

  private boolean canReconnect() {
    // onClose()
    if (wsListener.wsSocketStatus == 1006 /* Abnormal closure */
        || wsListener.wsSocketStatus == 1012 /* Service restart */
        || wsListener.wsSocketStatus == 1011 /* Unexpected condition. */) {
      return true;
    }
    // onError()
    if (wsListener.error != null
        && wsListener.error.getMessage().equals("Operation timed out")) {
      return true;
    }

    return false;
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

    int wsSocketStatus;
    String wsCloseReason;
    Throwable error;

    StringBuilder text = new StringBuilder();

    @Override
    public void onOpen(WebSocket webSocket) {
      LOG.debug("[{}] WebSocket onOpen()", Thread.currentThread().getName());
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
      LOG.debug("[{}] WebSocket onClose(), Code:{}", Thread.currentThread().getName(), statusCode);
      wsSocketStatus = statusCode;
      wsCloseReason = reason;
      return WebSocket.Listener.super.onClose(webSocket, statusCode, reason);
    }

    @Override
    public void onError(WebSocket webSocket, Throwable err) {
      LOG.debug("[{}] WebSocket onError(), Error: {}", Thread.currentThread().getName(), error);
      error = err;
    }
  }
}
