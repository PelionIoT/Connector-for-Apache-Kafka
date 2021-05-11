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
import com.pelion.connect.dm.schemas.AsyncIDResponseData;
import com.pelion.connect.dm.schemas.EndpointData;
import com.pelion.connect.dm.schemas.NotificationData;
import com.pelion.connect.dm.utils.PelionAPI;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
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
      // would also be used as a record key
      String ep = jsonNode.get("ep").asText();

      Struct ndStruct = new Struct(NotificationData.SCHEMA)
          .put(NotificationData.EP_FIELD, ep)
          .put(NotificationData.PATH_FIELD, jsonNode.get("path").asText())
          .put(NotificationData.CT_FIELD, jsonNode.get("ct").asText())
          .put(NotificationData.PAYLOAD_B64_FIELD, jsonNode.get("payload").asText())
          .put(NotificationData.MAX_AGE_FIELD, jsonNode.get("max-age").asInt())
          .put(NotificationData.UID_FIELD, jsonNode.get("uid").asText())
          .put(NotificationData.TIMESTAMP_FIELD, jsonNode.get("timestamp").asLong())
          .put(NotificationData.ORIGINAL_EP_FIELD, jsonNode.get("original-ep").asText());

      // determine resource
      String resource = jsonNode.get("path").asText().split("/")[3];
      // create connect struct
      Struct payloadStruct = new Struct(NotificationData.PayloadData.SCHEMA);

      // check the type associated with the resource
      if (mappingExists(INTEGER, resource)) {
        payloadStruct.put(NotificationData.PayloadData.L_FIELD,
            Long.parseLong(base64Decode(jsonNode.get("payload").asText())));
      } else if (mappingExists(DOUBLE, resource)) {
        payloadStruct.put(NotificationData.PayloadData.D_FIELD,
            Double.parseDouble(base64Decode(jsonNode.get("payload").asText())));
      } else if (mappingExists(BOOLEAN, resource)) {
        payloadStruct.put(NotificationData.PayloadData.B_FIELD,
            Boolean.parseBoolean(base64Decode(jsonNode.get("payload").asText())));
      } else { // treat it as a generic string
        payloadStruct.put(NotificationData.PayloadData.S_FIELD,
            base64Decode(jsonNode.get("payload").asText()));
      }

      // assign the payload
      ndStruct.put(NotificationData.PAYLOAD_FIELD, payloadStruct);

      // build the connect record
      SourceRecord record = new SourceRecord(
          null,
          null,
          this.ndTopic,
          null,
          SchemaBuilder.string().build(),
          ep,
          NotificationData.SCHEMA,
          ndStruct);

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
      // would also be used as a record key
      String ep = jsonNode.get("ep").asText();

      Struct ndStruct = new Struct(EndpointData.SCHEMA)
          .put(EndpointData.EP_FIELD, ep)
          .put(EndpointData.ORIGINAL_EP_FIELD, jsonNode.get("original-ep").asText())
          .put(EndpointData.EP_TYPE_FIELD, jsonNode.path("ept").asText())
          .put(EndpointData.QUEUE_FIELD, jsonNode.path("q").asBoolean())
          .put(EndpointData.TIMESTAMP_FIELD, jsonNode.path("timestamp").asLong());

      // traverse resources
      final JsonNode resources = jsonNode.get("resources");
      final List<Struct> list = new ArrayList<>(resources.size());
      for (final JsonNode r : resources) {
        // create connect struct
        Struct resStruct = new Struct(EndpointData.ResourceDataSchema.SCHEMA);
        resStruct.put(EndpointData.ResourceDataSchema.PATH_FIELD, r.get("path").asText());
        if (r.has("if")) {
          resStruct.put(EndpointData.ResourceDataSchema.IF_FIELD, r.get("if").asText());
        }
        if (r.has("rt")) {
          resStruct.put(EndpointData.ResourceDataSchema.RT_FIELD, r.get("rt").asText());
        }
        if (r.has("ct")) {
          resStruct.put(EndpointData.ResourceDataSchema.CT_FIELD, r.get("ct").asText());
        }
        if (r.has("obs")) {
          resStruct.put(EndpointData.ResourceDataSchema.OBS_FIELD, r.get("obs").asBoolean());
        }
        // add it to the list
        list.add(resStruct);
      }

      // assign the list
      ndStruct.put(EndpointData.RD_FIELD, list);

      // build the connect record
      SourceRecord record = new SourceRecord(
          null,
          null,
          this.edTopic,
          null,
          SchemaBuilder.string().build(),
          ep,
          EndpointData.SCHEMA,
          ndStruct);

      // add it to our list
      records.add(record);
    }
  }

  private void processAsyncResponses(List<SourceRecord> records, JsonNode jsonData) {
    for (final JsonNode jsonNode : jsonData) {

      // would also be used as a record key
      String id = jsonNode.get("id").asText();

      Struct responseStruct = new Struct(AsyncIDResponseData.SCHEMA)
          .put(AsyncIDResponseData.ID_FIELD, id)
          .put(AsyncIDResponseData.PAYLOAD_FIELD, base64Decode(jsonNode.get("payload").asText()))
          .put(AsyncIDResponseData.STATUS_FIELD, jsonNode.get("status").asInt());

      if (jsonNode.has("error")) {
        responseStruct.put(AsyncIDResponseData.ERROR_FIELD, jsonNode.get("error").asText());
      }
      if (jsonNode.has("ct")) {
        responseStruct.put(AsyncIDResponseData.CT_FIELD, jsonNode.get("ct").asText());
      }
      if (jsonNode.has("max-age")) {
        responseStruct.put(AsyncIDResponseData.MAX_AGE_FIELD, jsonNode.get("max-age").asInt());
      }

      // build the connect record
      SourceRecord record = new SourceRecord(
          null,
          null,
          this.arTopic,
          null,
          SchemaBuilder.string().build(),
          id,
          AsyncIDResponseData.SCHEMA,
          responseStruct);

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

    @SuppressWarnings("checkstyle:BooleanExpressionComplexity")
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
          LOG.trace("[{}] sending keepalive ping", Thread.currentThread().getName());
          websocket.sendPing(pingMsg)
              .exceptionally(ex -> {
                LOG.error("[{}] sending keepalive ping error '{}'", Thread.currentThread().getName(), ex.getMessage());
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

      // clear queue
      queue.clear();
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
      LOG.trace("[{}] WebSocket onOpen()", Thread.currentThread().getName());
      WebSocket.Listener.super.onOpen(webSocket);
    }

    @Override
    public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
      text.append(data);
      if (last) {
        queue.add(text.toString());
        LOG.trace("[{}] {}", Thread.currentThread().getName(), text);
        text.setLength(0); // clear buffer
      }
      return WebSocket.Listener.super.onText(webSocket, data, last);
    }

    @Override
    public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
      LOG.trace("[{}] WebSocket onClose(), code:{}, reason:{}", Thread.currentThread().getName(), statusCode, reason);
      lastStatusCode = statusCode; // should contain Pelion error response code
      lastCloseReason = reason;
      return WebSocket.Listener.super.onClose(webSocket, statusCode, reason);
    }

    @Override
    public void onError(WebSocket webSocket, Throwable error) {
      LOG.error("[{}] WebSocket onError(), Error: {}", Thread.currentThread().getName(), error.getMessage());
      lastStatusCode = -1; // flag for any other errors
      lastCloseReason = error.getMessage();
    }

    public void clearErrors() {
      lastStatusCode = 0;
      lastCloseReason = null;
    }
  }
}
