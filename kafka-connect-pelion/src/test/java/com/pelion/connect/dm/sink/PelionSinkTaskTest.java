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

import com.pelion.connect.dm.exception.RequestFailedException;
import com.pelion.connect.dm.utils.PelionAPI;
import com.pelion.protobuf.PelionProtos.DeviceRequest.Body.Method;
import io.confluent.connect.protobuf.ProtobufData;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.pelion.protobuf.PelionProtos.DeviceRequest;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class PelionSinkTaskTest {

  protected static final String TOPIC = "connect-pelion-sink-requests";

  private PelionSinkTask task;

  private final PelionAPI pelionAPI = mock(PelionAPI.class);

  protected static final ProtobufSchema PROTOBUF_REQ_SCHEMA = new ProtobufSchema(
      DeviceRequest.getDescriptor());

  protected static final ProtobufData protobufData = new ProtobufData();

  @Before
  public void setup() {
    Map<String, String> props = new HashMap<>();
    props.put(PelionSinkConnectorConfig.PELION_API_HOST_CONFIG, "api.us-east-1.mbedcloud.com");
    props.put(PelionSinkTaskConfig.PELION_ACCESS_KEY_CONFIG, "key1");
    props.put(PelionSinkTaskConfig.MAX_RETRIES, "2");
    props.put(PelionSinkTaskConfig.RETRY_BACKOFF_MS, "100");

    task = new PelionSinkTask();
    task.start(props, pelionAPI);
  }

  @Test
  public void shouldCorrectlyExecuteDeviceRequest() {
    DeviceRequest request = buildDeviceRequest();
    SchemaAndValue schemaAndValue = protobufData
        .toConnectData(PROTOBUF_REQ_SCHEMA, request);

    SinkRecord record = new SinkRecord(
        TOPIC,
        1,
        Schema.STRING_SCHEMA,
        "id",
        schemaAndValue.schema(),
        schemaAndValue.value(),
        0);

    task.put(Collections.singletonList(record));
    verify(pelionAPI).executeDeviceRequest(request);
  }

  @Test(expected = ConnectException.class)
  public void shouldBackoffAndFailAfterFailingToExecuteDeviceRequestCauseOfIOError() {
    DeviceRequest request = buildDeviceRequest();
    SchemaAndValue schemaAndValue = protobufData
        .toConnectData(PROTOBUF_REQ_SCHEMA, request);

    SinkRecord record = new SinkRecord(
        TOPIC,
        1,
        Schema.STRING_SCHEMA,
        "id",
        schemaAndValue.schema(),
        schemaAndValue.value(),
        0);

    // mock an IO error
    RequestFailedException rfe = new RequestFailedException(0, "path", "request", "body",
        new IOException(), "connection error");
    when(pelionAPI.executeDeviceRequest(request)).thenReturn(rfe);

    task.put(Collections.singletonList(record));
    // should have reached 0
    assertEquals(task.getRetries(), 0);
  }

  @Test(expected = ConnectException.class)
  public void shouldFailAfterFailingToExecuteDeviceRequest() {
    DeviceRequest request = buildDeviceRequest();
    SchemaAndValue schemaAndValue = protobufData
        .toConnectData(PROTOBUF_REQ_SCHEMA, request);

    SinkRecord record = new SinkRecord(
        TOPIC,
        1,
        Schema.STRING_SCHEMA,
        "id",
        schemaAndValue.schema(),
        schemaAndValue.value(),
        0);

    // mock a failure request
    RequestFailedException rfe = new RequestFailedException(404, "path", "request", "body", null, "");
    when(pelionAPI.executeDeviceRequest(request)).thenReturn(rfe);

    task.put(Collections.singletonList(record));
    // task should fail immediately cause of unrecoverable error
    assertEquals(task.getRetries(), 2);
  }

  private DeviceRequest buildDeviceRequest() {
    DeviceRequest.Builder request = DeviceRequest.newBuilder();

    request.setEp("01767982c9250000000000010011579e");
    request.setAsyncId("foobar");
    DeviceRequest.Body.Builder body = DeviceRequest.Body.newBuilder();
    body.setMethod(Method.GET);
    body.setUri("/3200/0/5501");
    request.setBody(body.build());

    return request.build();
  }
}