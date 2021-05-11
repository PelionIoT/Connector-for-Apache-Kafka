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
import com.pelion.connect.dm.schemas.DeviceRequestData;
import com.pelion.connect.dm.utils.PelionAPI;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PelionSinkTaskTest {

  protected static final String TOPIC = "connect-pelion-sink-requests";

  private PelionSinkTask task;

  private final PelionAPI pelionAPI = mock(PelionAPI.class);

  @Before
  public void setup() {
    Map<String, String> props = getDefaultProps();
    task = new PelionSinkTask();
    task.start(props, pelionAPI);
  }

  @Test
  public void shouldCorrectlyExecuteDeviceRequest() {
    Struct request = buildDeviceRequest();

    SinkRecord record = new SinkRecord(
        TOPIC,
        1,
        Schema.STRING_SCHEMA,
        "id",
        request.schema(),
        request,
        0);

    task.put(Collections.singletonList(record));
    verify(pelionAPI).executeDeviceRequest(request);
  }

  @Test(expected = ConnectException.class)
  public void shouldBackoffAndFailAfterFailingToExecuteDeviceRequestCauseOfIOError() {
    Struct request = buildDeviceRequest();

    SinkRecord record = new SinkRecord(
        TOPIC,
        1,
        Schema.STRING_SCHEMA,
        "id",
        request.schema(),
        request,
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
  public void shouldFailAfterFailingToExecuteDeviceRequestIfIgnoreErrorsIsFalse() {
    Struct request = buildDeviceRequest();

    SinkRecord record = new SinkRecord(
        TOPIC,
        1,
        Schema.STRING_SCHEMA,
        "id",
        request.schema(),
        request,
        0);

    // mock a failure request
    RequestFailedException rfe = new RequestFailedException(404, "path", "request", "body", null, "");
    when(pelionAPI.executeDeviceRequest(request)).thenReturn(rfe);

    task.put(Collections.singletonList(record));
    // task should fail immediately cause of unrecoverable error
    assertEquals(task.getRetries(), 2);
  }

  @Test
  public void shouldContinueAfterFailingToExecuteDeviceRequestIfIgnoreErrorsIsTrue() {
    Map<String, String> props = getDefaultProps();
    props.put(PelionSinkTaskConfig.IGNORE_ERRORS, "true");

    task = new PelionSinkTask();
    task.start(props, pelionAPI);

    Struct request = buildDeviceRequest();

    SinkRecord record = new SinkRecord(
        TOPIC,
        1,
        Schema.STRING_SCHEMA,
        "id",
        request.schema(),
        request,
        0);

    // mock a failure request
    RequestFailedException rfe = new RequestFailedException(400, "path", "request", "RESOURCE_NOT_FOUND", null, "");
    when(pelionAPI.executeDeviceRequest(request)).thenReturn(rfe);

    task.put(Collections.singletonList(record));
    verify(pelionAPI).executeDeviceRequest(request);
  }

  private Struct buildDeviceRequest() {
    Struct request = new Struct(DeviceRequestData.SCHEMA);

    // the endpoint
    request.put(DeviceRequestData.EP_FIELD, "01767982c9250000000000010011579e");

    // the body
    Struct body = new Struct((DeviceRequestData.BodyData.SCHEMA));
    body.put(DeviceRequestData.BodyData.METHOD_FIELD, "GET");
    body.put(DeviceRequestData.BodyData.URI_FIELD, "/3200/0/5501");

    // assign the body
    request.put(DeviceRequestData.BODY_FIELD, body);

    return request;
  }

  private Map<String, String> getDefaultProps() {
    Map<String, String> props = new HashMap<>();
    props.put(PelionSinkConnectorConfig.PELION_API_HOST_CONFIG, "api.us-east-1.mbedcloud.com");
    props.put(PelionSinkTaskConfig.PELION_ACCESS_KEY_CONFIG, "key1");
    props.put(PelionSinkTaskConfig.MAX_RETRIES, "2");
    props.put(PelionSinkTaskConfig.RETRY_BACKOFF_MS, "100");
    props.put(PelionSinkTaskConfig.IGNORE_ERRORS, "false");

    return props;
  }
}