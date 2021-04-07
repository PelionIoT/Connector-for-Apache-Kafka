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

package com.pelion.connect.dm.sink;

import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.pelion.connect.dm.exception.RequestFailedException;
import com.pelion.connect.dm.utils.PelionAPI;
import com.pelion.protobuf.PelionProtos.DeviceRequest;
import io.confluent.connect.protobuf.ProtobufData;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import static com.pelion.connect.dm.utils.PelionConnectorUtils.getVersion;
import static com.pelion.connect.dm.utils.PelionConnectorUtils.readFile;
import static com.pelion.connect.dm.utils.PelionConnectorUtils.sleep;

public class PelionSinkTask extends SinkTask {

  private static final Logger LOG = LoggerFactory.getLogger(PelionSinkTask.class);

  private PelionSinkTaskConfig config;
  private PelionAPI pelionAPI;

  private int retries;
  private int maxRetries;
  private int retryBackoffMs;
  private boolean ignoreErrors;

  private static final ProtobufData protobufData = new ProtobufData();

  @Override
  public String version() {
    return getVersion();
  }

  @Override
  public void start(Map<String, String> props) {
    start(props, null);
  }

  // visible for testing
  public void start(Map<String, String> props, PelionAPI pelionAPI) {
    LOG.info(readFile("pelion-sink-ascii.txt"));

    // initialize config
    this.config = new PelionSinkTaskConfig(props);
    maxRetries = config.getInt(PelionSinkTaskConfig.MAX_RETRIES);
    retryBackoffMs = config.getInt(PelionSinkTaskConfig.RETRY_BACKOFF_MS);
    ignoreErrors = config.getBoolean(PelionSinkTaskConfig.IGNORE_ERRORS);

    // initialize api engine
    this.pelionAPI = pelionAPI != null ? pelionAPI :
        new PelionAPI(config.getString(PelionSinkConnectorConfig.PELION_API_HOST_CONFIG),
            config.getPassword(PelionSinkConnectorConfig.PELION_ACCESS_KEY_CONFIG).value());

    LOG.info("[{}] Started Pelion sink task", Thread.currentThread().getName());
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    if (records.isEmpty()) {
      return;
    }

    final SinkRecord first = records.iterator().next();
    final int recordsCount = records.size();
    LOG.trace("[{}] received {} records with first record kafka coordinates:(topic:{},partition:{},offset:{}).",
        Thread.currentThread().getName(), recordsCount, first.topic(), first.kafkaPartition(), first.kafkaOffset());

    records.stream().map(this::asDeviceRequest).forEach(request -> {
      while (true) { // loop to allow retries
        RequestFailedException rfe = pelionAPI.executeDeviceRequest(request);
        if (rfe == null) {
          break;
        }

        // process failed request

        // for I/O errors (eg. unable to connect) backoff and retry
        if (rfe.exception() instanceof IOException) {
          // throw error if no more retries left
          if (retries == maxRetries) {
            throw new ConnectException(String.format("[%s] exceeded the maximum number of retries (%d)",
                Thread.currentThread().getName(), maxRetries), rfe);
          }

          retries++;
          int millis = retries * retryBackoffMs;
          LOG.debug("[{}] backing off after failing to execute device request (reason: {}), sleeping for {} ms, retries {}",
              Thread.currentThread().getName(), rfe.getMessage(), millis, retries);
          sleep(millis);

        } else { // Pelion replied with error status
          LOG.debug("[{}] request with async-id '{}' throw an error: {}",
              Thread.currentThread().getName(), request.getAsyncId(), rfe.getMessage());
          if (!this.ignoreErrors) { // should we stop ?
            throw new ConnectException(rfe);
          } else {
            break; // continue processing
          }
        }
      }
      retries = maxRetries;
    });
  }

  @Override
  public void stop() {
    LOG.info("[{}] Stopping Pelion sink task", Thread.currentThread().getName());
  }

  public int getRetries() {
    return retries;
  }

  private DeviceRequest asDeviceRequest(SinkRecord record) {
    DynamicMessage msg = (DynamicMessage) protobufData
        .fromConnectData(record.valueSchema(), record.value())
        .getValue();

    try {
      return DeviceRequest.parseFrom(msg.toByteArray());
    } catch (InvalidProtocolBufferException e) {
      e.printStackTrace();
    }

    return null;
  }
}
