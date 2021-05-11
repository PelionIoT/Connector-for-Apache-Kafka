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

import com.pelion.connect.dm.utils.PelionAPI;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.http.WebSocket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PelionSourceTaskTest {

  private PelionSourceTask task;

  private final BlockingQueue<String> queue = new LinkedBlockingQueue<>();

  // mock Pelion API
  private final PelionAPI mockAPI = mock(PelionAPI.class);
  private final WebSocket mockWebSocket = mock(WebSocket.class);

  private PelionSourceTask.WebSocketListener listener = new PelionSourceTask.WebSocketListener(queue);

  @Before
  public void setup() throws InterruptedException, ExecutionException {
    Map<String, String> props = getDefaultProps();
    task = new PelionSourceTask();

    // mock Websocket to not allow retries to kick
    when(mockAPI.connectNotificationChannel(any())).thenReturn(mockWebSocket);

    task.start(props, mockAPI, listener, queue, 1);
  }

  @Test
  public void shouldHandleNotificationMessages() throws Exception {
    final String notifMsg = "{\n" +
        "   \"notifications\":[\n" +
        "      {\n" +
        "         \"ep\":\"01767982c9250000000000010011579e\",\n" +
        "         \"path\":\"/3200/0/5501\",\n" +
        "         \"ct\":\"text/plain\",\n" +
        "         \"payload\":\"NDA2\",\n" +
        "         \"max-age\":0,\n" +
        "         \"uid\":\"0d8e08c3-2311-4ede-aa53-fdaff2b3fad3\",\n" +
        "         \"timestamp\":1614180566644,\n" +
        "         \"original-ep\":\"01767982c9250000000000010011579e\"\n" +
        "      },\n" +
        "      {\n" +
        "         \"ep\":\"0176c7561cf3000000000001001122d4\",\n" +
        "         \"path\":\"/3201/0/5853\",\n" +
        "         \"ct\":\"text/plain\",\n" +
        "         \"payload\":\"MTAwOjEwMDoxMDA6MTAwOjEwMDoxMDA=\",\n" +
        "         \"max-age\":0,\n" +
        "         \"uid\":\"0f2e03b5-1455-3nce-ba53-adacd4c2waf1\",\n" +
        "         \"timestamp\":1614180568514," +
        "         \"original-ep\":\"0176c7561cf3000000000001001122d4\"\n" +
        "      }\n" +
        "   ]\n" +
        "}";
    queue.add(notifMsg);

    List<SourceRecord> records = task.poll();
    assertEquals(2, records.size());

    Struct obj;

    obj = (Struct) records.get(0).value();
    assertEquals("01767982c9250000000000010011579e", obj.get("ep"));
    assertEquals("/3200/0/5501", obj.get("path"));
    assertEquals("text/plain", obj.get("ct"));
    assertEquals("NDA2", obj.get("payload_b64"));
    assertEquals(0, obj.get("max_age"));
    assertEquals("0d8e08c3-2311-4ede-aa53-fdaff2b3fad3", obj.get("uid"));
    assertEquals(1614180566644L, obj.get("timestamp"));
    assertEquals("01767982c9250000000000010011579e", obj.get("original_ep"));
    // the generated payload should be integer according to the provided mapping
    assertEquals(406L, obj.getStruct("payload").get("l"));

    obj = (Struct) records.get(1).value();
    assertEquals("0176c7561cf3000000000001001122d4", obj.get("ep"));
    assertEquals("/3201/0/5853", obj.get("path"));
    assertEquals("text/plain", obj.get("ct"));
    assertEquals("MTAwOjEwMDoxMDA6MTAwOjEwMDoxMDA=", obj.get("payload_b64"));
    assertEquals(0, obj.get("max_age"));
    assertEquals("0f2e03b5-1455-3nce-ba53-adacd4c2waf1", obj.get("uid"));
    assertEquals(1614180568514L, obj.get("timestamp"));
    assertEquals("0176c7561cf3000000000001001122d4", obj.get("original_ep"));
    // the generated payload should be string according to the provided mapping
    assertEquals("100:100:100:100:100:100", obj.getStruct("payload").get("s"));
  }

  @Test
  public void shouldHandleRegistrationMessages() throws Exception {
    final String regMsg = "{\n" +
        "   \"registrations\":[\n" +
        "      {\n" +
        "         \"ep\":\"01767982c9250000000000010011579e\",\n" +
        "         \"original-ep\":\"01767982c9250000000000010011579e\",\n" +
        "         \"ept\":\"default\",\n" +
        "         \"resources\":[\n" +
        "            {\n" +
        "               \"path\":\"/10255/0/4\",\n" +
        "               \"rt\":\"Class\",\n" +
        "               \"ct\":\"application/vnd.oma.lwm2m+tlv\",\n" +
        "               \"obs\":true\n" +
        "            },\n" +
        "            {\n" +
        "               \"path\":\"/10255/0/3\",\n" +
        "               \"rt\":\"Vendor\",\n" +
        "               \"obs\":true\n" +
        "            }\n" +
        "         ],\n" +
        "         \"timestamp\":\"1614584011482\"\n" +
        "      },\n" +
        "      {\n" +
        "         \"ep\":\"0176c7561cf3000000000001001122d4\",\n" +
        "         \"original-ep\":\"0176c7561cf3000000000001001122d4\",\n" +
        "         \"ept\":\"default\",\n" +
        "         \"resources\":[\n" +
        "            {\n" +
        "               \"path\":\"/10255/0/4\",\n" +
        "               \"rt\":\"Class\",\n" +
        "               \"ct\":\"application/vnd.oma.lwm2m+tlv\",\n" +
        "               \"obs\":true\n" +
        "            },\n" +
        "            {\n" +
        "               \"path\":\"/10255/0/3\",\n" +
        "               \"rt\":\"Vendor\",\n" +
        "               \"obs\":true\n" +
        "            }\n" +
        "         ],\n" +
        "         \"timestamp\":\"1614584011482\"\n" +
        "      }\n" +
        "   ]\n" +
        "}";

    queue.add(regMsg);

    List<SourceRecord> records = task.poll();
    assertEquals(2, records.size());

    Struct obj;
    List<Struct> resources;
    obj = (Struct) records.get(0).value();
    assertEquals("01767982c9250000000000010011579e", obj.get("ep"));
    assertEquals("01767982c9250000000000010011579e", obj.get("original_ep"));
    assertEquals("default", obj.get("ept"));

    resources = obj.getArray("resources");
    assertEquals(2, resources.size());

    obj = resources.get(0);
    assertEquals("/10255/0/4", obj.get("path"));
    assertEquals("Class", obj.get("rt"));
    assertEquals("application/vnd.oma.lwm2m+tlv", obj.get("ct"));
    assertEquals(true, obj.get("obs"));

    obj = resources.get(1);
    assertEquals("/10255/0/3", obj.get("path"));
    assertEquals("Vendor", obj.get("rt"));
    assertNull(obj.get("ct"));
    assertEquals(true, obj.get("obs"));

    obj = (Struct) records.get(1).value();
    assertEquals("0176c7561cf3000000000001001122d4", obj.get("ep"));
    assertEquals("0176c7561cf3000000000001001122d4", obj.get("original_ep"));
    assertEquals("default", obj.get("ept"));

    resources = obj.getArray("resources");
    assertEquals(2, resources.size());

    obj = resources.get(0);
    assertEquals("/10255/0/4", obj.get("path"));
    assertEquals("Class", obj.get("rt"));
    assertEquals("application/vnd.oma.lwm2m+tlv", obj.get("ct"));
    assertEquals(true, obj.get("obs"));

    obj = resources.get(1);
    assertEquals("/10255/0/3", obj.get("path"));
    assertEquals("Vendor", obj.get("rt"));
    assertNull(obj.get("ct"));
    assertEquals(true, obj.get("obs"));
  }

  @Test
  public void shouldHandleResponseMessage() throws Exception {
    String respMsg = "{\n" +
        "   \"async-responses\":[\n" +
        "      {\n" +
        "         \"id\":\"foobar\",\n" +
        "         \"status\":200,\n" +
        "         \"payload\":\"NjAw\",\n" +
        "         \"ct\":\"text/plain\",\n" +
        "         \"max-age\":0\n" +
        "      }\n" +
        "   ]\n" +
        "}";

    queue.add(respMsg);

    List<SourceRecord> records = task.poll();
    assertEquals(1, records.size());

    Struct obj = (Struct) records.get(0).value();
    assertEquals("foobar", obj.get("id"));
    assertEquals(200, obj.get("status"));
    assertEquals("600", obj.get("payload"));
    assertEquals("text/plain", obj.get("ct"));
    assertEquals(0, obj.get("max_age"));
  }

  @Test
  public void shouldBackoffAndFailAfterFailingToConnectWebsocketDuringSetupCauseOfError() throws Exception {
    Map<String, String> props = getDefaultProps();
    task = new PelionSourceTask();

    // mock Pelion API
    PelionAPI mockAPI = mock(PelionAPI.class);
    // throw IOException to signal error when connecting notif. channel
    doThrow(new ExecutionException(new IOException())).when(mockAPI).connectNotificationChannel(any());
    // start task should throw exception cause unable to connect after backing off and retrying
    assertThrows(ConnectException.class, () -> {
      task.start(props, mockAPI, listener, queue, 1);
    });
    // retries should have reached :2
    assertEquals(2, task.getRetries());
  }

  @Test
  public void shouldBackoffAndFailAfterFailingToConnectWebsocketMidFlightCauseOfError() throws Exception {
    final String notifMsg = "{\n" +
        "   \"notifications\":[\n" +
        "      {\n" +
        "         \"ep\":\"01767982c9250000000000010011579e\",\n" +
        "         \"path\":\"/3200/0/5501\",\n" +
        "         \"ct\":\"text/plain\",\n" +
        "         \"payload\":\"NDA2\",\n" +
        "         \"max-age\":0,\n" +
        "         \"uid\":\"0d8e08c3-2311-4ede-aa53-fdaff2b3fad3\",\n" +
        "         \"timestamp\":1614180566644,\n" +
        "         \"original-ep\":\"01767982c9250000000000010011579e\"\n" +
        "      },\n" +
        "      {\n" +
        "         \"ep\":\"0176c7561cf3000000000001001122d4\",\n" +
        "         \"path\":\"/3201/0/5853\",\n" +
        "         \"ct\":\"text/plain\",\n" +
        "         \"payload\":\"MTAwOjEwMDoxMDA6MTAwOjEwMDoxMDA=\",\n" +
        "         \"max-age\":0,\n" +
        "         \"uid\":\"0f2e03b5-1455-3nce-ba53-adacd4c2waf1\",\n" +
        "         \"timestamp\":1614180568514," +
        "         \"original-ep\":\"0176c7561cf3000000000001001122d4\"\n" +
        "      }\n" +
        "   ]\n" +
        "}";
    queue.add(notifMsg);

    List<SourceRecord> records;
    // should retrieve records first time
    records = task.poll();
    assertEquals(2, records.size());

    // simulate websocket closing cause of error
    when(mockWebSocket.isInputClosed()).thenReturn(true);

    // try to get records
    // poll should throw exception
    assertThrows(ConnectException.class, () -> {
      // simulate successive polls
      for (int i = 0; i < 3; i++) {
        // continue failing
        listener.lastCloseReason = "Connection reset by peer";
        listener.lastStatusCode = -1;
        task.poll(); // backoff
      }
    });

    // retries should have reached 0
    assertEquals(2, task.getRetries());
  }

  private Map<String, String> getDefaultProps() {
    Map<String, String> props = new HashMap<>();
    props.put(PelionSourceConnectorConfig.PELION_ACCESS_KEY_LIST_CONFIG, "key1, key2");
    props.put(PelionSourceConnectorConfig.TOPIC_PREFIX, "mypelion");
    props.put(PelionSourceConnectorConfig.SUBSCRIPTIONS_CONFIG, "presub1, presub2, presub3, presub4, presub5");
    props.put(PelionSourceConnectorConfig.RESOURCE_TYPE_MAPPING_CONFIG, "1:i, 5501:i, 21:i, 5853:s");
    props.put(PelionSourceConnectorConfig.MAX_RETRIES, "2");
    props.put(PelionSourceConnectorConfig.RETRY_BACKOFF_MS, "100");
    props.put("subscriptions.presub1.endpoint-name", "01767982c9250000000000010011579e");
    props.put("subscriptions.presub1.resource-path", "/3200/0/5501, /3201/0/5853");
    props.put("subscriptions.presub2.endpoint-type", "Light");
    props.put("subscriptions.presub2.resource-path", "sen/*");
    props.put("subscriptions.presub3.endpoint-type", "Sensor");
    props.put("subscriptions.presub4.resource-path", "/dev/temp, /dev/hum");
    props.put("subscriptions.presub5.endpoint-name", "0176c7561cf3000000000001001122d4");
    // enable registration publishing for this task
    props.put("publish_registrations", "true");

    return props;
  }
}
