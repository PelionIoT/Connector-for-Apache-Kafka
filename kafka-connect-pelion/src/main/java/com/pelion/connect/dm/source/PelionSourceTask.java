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
import com.pelion.connect.dm.utils.PelionAPI;
import com.pelion.protobuf.PelionProtos.AsyncIDResponse;
import com.pelion.protobuf.PelionProtos.EndpointData;
import com.pelion.protobuf.PelionProtos.NotificationData;
import com.pelion.protobuf.PelionProtos.ResourceData;
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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.pelion.connect.dm.utils.PelionConnectorUtils.BOOLEAN;
import static com.pelion.connect.dm.utils.PelionConnectorUtils.DOUBLE;
import static com.pelion.connect.dm.utils.PelionConnectorUtils.INTEGER;
import static com.pelion.connect.dm.utils.PelionConnectorUtils.base64Decode;
import static com.pelion.connect.dm.utils.PelionConnectorUtils.getVersion;
import static com.pelion.connect.dm.utils.PelionConnectorUtils.mapper;
import static com.pelion.connect.dm.utils.PelionConnectorUtils.protobufData;
import static com.pelion.connect.dm.utils.PelionConnectorUtils.readFile;
import static com.pelion.connect.dm.utils.PelionConnectorUtils.sleep;
import static com.pelion.connect.dm.utils.PelionConnectorUtils.uniqueIndex;

public class PelionSourceTask extends SourceTask {

  private static final Logger LOG = LoggerFactory.getLogger(PelionSourceTask.class);

  private BlockingQueue<String> queue;

  private PelionAPI pelionAPI;

  private String ndTopic;
  private String edTopic;
  private String arTopic;

  private int retries;
  private int maxRetries;
  private int retryBackoffMs;

  private boolean shouldPublishRegistrations;

  private Map<String, List<String>> types;

  private PelionNotificationChannelHandler pelionChannel;

  private int pollTimeout;

  @Override
  public String version() {
    return getVersion();
  }

  public void start(Map<String, String> props) {
    start(props, null, null, null, 10);
  }

  // visible for testing
  public void start(Map<String, String> props, PelionAPI api, WebSocketListener listener, BlockingQueue<String> q, int timeout) {
    LOG.info(readFile("pelion-source-ascii.txt"));

    // initialize config
    final PelionSourceTaskConfig config = new PelionSourceTaskConfig(props);
    // the kafka topics where we store records
    // follows the schema: {TOPIC_PREFIX}-{notifications|registrations|responses}
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
    // retry mechanism
    this.maxRetries = config.getInt(PelionSourceConnectorConfig.MAX_RETRIES);
    this.retryBackoffMs = config.getInt(PelionSourceConnectorConfig.RETRY_BACKOFF_MS);
    // the queue that holds incoming messages
    this.queue = q != null ? q : new LinkedBlockingQueue<>();
    // the pelion notification channel handler
    this.pelionChannel = (listener == null ? new PelionNotificationChannelHandler() /* default */ :
        new PelionNotificationChannelHandler((listener))); /* for testing */
    this.pollTimeout = timeout;
    // the API engine
    this.pelionAPI = (api != null ? api :
        new PelionAPI(config.getString(PelionSourceConnectorConfig.PELION_API_HOST_CONFIG),
            config.getPassword(PelionSourceTaskConfig.PELION_TASK_ACCESS_KEY_CONFIG).value()));

    // setup pre-subscriptions
    pelionAPI.createPreSubscriptions(config);
    // connect to Pelion notification channel
    pelionChannel.connect();
    // ready
    LOG.info("[{}] Started Pelion source task", Thread.currentThread().getName());
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    // check whether ws channel was closed unexpectedly and reconnection is required
    pelionChannel.checkIfReconnectionIsNeeded();

    // now block until message is received or timeout on poll occurs. We
    // need to return regularly (with null) to allow task transitions (e.g PAUSE)
    final String message = queue.poll(pollTimeout, TimeUnit.SECONDS);
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
      pelionChannel.releaseResources();
      throw new ConnectException(e);
    }
  }

  @Override
  public void stop() {
    LOG.info("[{}] Stopping Pelion source task", Thread.currentThread().getName());
    pelionChannel.releaseResources();
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
      LOG.debug("[{}] skipping registration incoming message [shouldPublishRegistration=false]",
          Thread.currentThread().getName());
      return;
    }

    for (final JsonNode jsonNode : jsonData) {
      // build protobuf obj from json
      EndpointData.Builder endpoint = EndpointData.newBuilder();

      endpoint.setEp(jsonNode.get("ep").asText());
      endpoint.setOriginalEp(jsonNode.get("original-ep").asText());
      // note: we use 'path()' instead of 'get()' for optional fields
      endpoint.setEpt(jsonNode.path("ept").asText());
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

  // visible for testing
  public int getRetries() {
    return retries;
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

  class PelionNotificationChannelHandler {

    WebSocket websocket;
    WebSocketListener listener;

    ScheduledThreadPoolExecutor executorService;
    ScheduledFuture<?> keepAliveThread;

    final ByteBuffer pingMsg = ByteBuffer.wrap("ping".getBytes());

    PelionNotificationChannelHandler() {
      this(new WebSocketListener(PelionSourceTask.this.queue));
    }

    // visible for testing
    PelionNotificationChannelHandler(WebSocketListener listener) {
      this.listener = listener;
      this.executorService = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(1);
      // remove on cancel
      executorService.setRemoveOnCancelPolicy(true);
    }

    void connect() {
      try {
        // attempt to connect (sync call)
        websocket = pelionAPI.connectNotificationChannel(listener);
        // start ws ping keepalive to catch silent termination
        startKeepAlive();
      } catch (ExecutionException e) {
        backOffAndConnect();
      } catch (InterruptedException e) {
        throw new ConnectException(e);
      }
    }

    void backOffAndConnect() {
      // reached retry limit ?
      if (retries == maxRetries) {
        throw new ConnectException(String.format("[%s] exceeded the maximum number of retries (%d) to connect notification channel",
            Thread.currentThread().getName(), maxRetries));
      }

      // otherwise backoff and attempt to reconnect
      retries++;
      int millis = retries * retryBackoffMs;
      LOG.info("[{}] backing off after failing to connect notification channel, sleeping for {} ms, retries {}",
          Thread.currentThread().getName(), millis, retries);
      sleep(millis);

      // attempt to connect
      connect();
    }

    void checkIfReconnectionIsNeeded() {
      if (websocket != null) {
        if (websocket.isInputClosed()) { // notification channel closed abnormally ?
          LOG.info("[{}] notification channel closed abnormally, code:'{}', reason:'{}'", Thread.currentThread().getName(),
              listener.lastStatusCode, listener.lastCloseReason);
          // can we reconnect ?
          if (isReconnectPossible()) {
            releaseResources();
            backOffAndConnect();
          } else {
            throw new ConnectException(String.format("[%s] notification channel returned unrecoverable status, unable "
                    + "to auto-reconnect, code:'%d', reason:'%s'", Thread.currentThread().getName(),
                listener.lastStatusCode, listener.lastCloseReason));
          }
        } else { // up and running..
          retries = 0;  // reset retries flag (if any)
        }
      }
    }

    boolean isReconnectPossible() {
      // extract error code returned from Pelion to determine if reconnect is possible
      // refer to Pelion API '/v2/notification/websocket-connect' response codes
      return listener.lastStatusCode == -1      /* Silent termination */
          || listener.lastStatusCode == 1000    /* Normal closure */
          || listener.lastStatusCode == 1006    /* Abnormal closure */
          || listener.lastStatusCode == 1011    /* Unexpected condition */
          || listener.lastStatusCode == 1012;   /* Service restart */
    }

    void startKeepAlive() {
      // setup ping keepalive so that silent ws termination can be handled accordingly
      keepAliveThread = executorService.scheduleAtFixedRate(() -> {
        // if the channel is still open, send ping
        if (!websocket.isOutputClosed()) {
          LOG.debug("[{}] sending keepalive ping", Thread.currentThread().getName());
          websocket.sendPing(pingMsg)
              .exceptionally(ex -> {
                LOG.debug("[{}] sending keepalive ping error '{}'", Thread.currentThread().getName(), ex.getMessage());
                releaseResources();
                return null;
              });
        }
      }, 10, 10, TimeUnit.SECONDS);
    }

    void releaseResources() {
      // close keepalive
      if (keepAliveThread != null) {
        keepAliveThread.cancel(false);
      }
      // close websocket
      if (websocket != null && !websocket.isOutputClosed()) {
        websocket.sendClose(WebSocket.NORMAL_CLOSURE, "closing");
      }
      // clear listener last status
      listener.clearErrors();
    }
  }

  static class WebSocketListener implements WebSocket.Listener {
    BlockingQueue<String> queue;

    int lastStatusCode;
    String lastCloseReason;

    StringBuilder text = new StringBuilder();

    public WebSocketListener(BlockingQueue<String> queue) {
      this.queue = queue;
    }

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
        text.setLength(0); // clear buffer
      }
      return WebSocket.Listener.super.onText(webSocket, data, last);
    }

    @Override
    public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
      LOG.debug("[{}] WebSocket onClose(), code:{}, reason:{}", Thread.currentThread().getName(), statusCode, reason);
      lastStatusCode = statusCode; // should contain Pelion error response code
      lastCloseReason = reason;
      return WebSocket.Listener.super.onClose(webSocket, statusCode, reason);
    }

    @Override
    public void onError(WebSocket webSocket, Throwable error) {
      LOG.debug("[{}] WebSocket onError(), Error: {}", Thread.currentThread().getName(), error.getMessage());
      lastStatusCode = -1; // flag for any other errors
      lastCloseReason = error.getMessage();
    }

    public void clearErrors() {
      lastStatusCode = 0;
      lastCloseReason = null;
    }
  }
}
