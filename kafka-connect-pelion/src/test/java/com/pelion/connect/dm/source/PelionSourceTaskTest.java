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
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class PelionSourceTaskTest {

  private PelionSourceTask task;

  private final BlockingQueue<String> queue = new LinkedBlockingQueue<>();

  @Before
  public void setup() {
    Map<String, String> props = new HashMap<>();
    props.put(PelionSourceConnectorConfig.PELION_ACCESS_KEY_LIST_CONFIG, "key1, key2");
    props.put(PelionSourceConnectorConfig.TOPIC_PREFIX, "mypelion");
    props.put(PelionSourceConnectorConfig.SUBSCRIPTIONS_CONFIG, "presub1, presub2, presub3, presub4, presub5");
    props.put("subscriptions.presub1.endpoint-name", "01767982c9250000000000010011579e");
    props.put("subscriptions.presub1.resource-path", "/3200/0/5501, /3201/0/5853");
    props.put("subscriptions.presub2.endpoint-type", "Light");
    props.put("subscriptions.presub2.resource-path", "sen/*");
    props.put("subscriptions.presub3.endpoint-type", "Sensor");
    props.put("subscriptions.presub4.resource-path", "/dev/temp, /dev/hum");
    props.put("subscriptions.presub5.endpoint-name", "0176c7561cf3000000000001001122d4");
    // enable registration publishing for this task
    props.put("publish_registrations", "true");

    task = new PelionSourceTask();
    task.start(props, mock(PelionAPI.class), queue);
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
        "         \"path\":\"/3200/0/5501\",\n" +
        "         \"ct\":\"text/plain\",\n" +
        "         \"payload\":\"NTA4\",\n" +
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
    assertEquals("406", obj.get("payload"));
    assertEquals(0, obj.get("max_age"));
    assertEquals("0d8e08c3-2311-4ede-aa53-fdaff2b3fad3", obj.get("uid"));
    assertEquals(1614180566644L, obj.get("timestamp"));
    assertEquals("01767982c9250000000000010011579e", obj.get("original_ep"));

    obj = (Struct) records.get(1).value();
    assertEquals("0176c7561cf3000000000001001122d4", obj.get("ep"));
    assertEquals("/3200/0/5501", obj.get("path"));
    assertEquals("text/plain", obj.get("ct"));
    assertEquals("508", obj.get("payload"));
    assertEquals(0, obj.get("max_age"));
    assertEquals("0f2e03b5-1455-3nce-ba53-adacd4c2waf1", obj.get("uid"));
    assertEquals(1614180568514L, obj.get("timestamp"));
    assertEquals("0176c7561cf3000000000001001122d4", obj.get("original_ep"));
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

    resources = obj.getArray("resource");
    assertEquals(2, resources.size());

    obj = resources.get(0);
    assertEquals("/10255/0/4", obj.get("path"));
    assertEquals("Class", obj.get("rt"));
    assertEquals("application/vnd.oma.lwm2m+tlv", obj.get("ct"));
    assertEquals(true, obj.get("obs"));

    obj = resources.get(1);
    assertEquals("/10255/0/3", obj.get("path"));
    assertEquals("Vendor", obj.get("rt"));
    assertEquals("", obj.get("ct"));
    assertEquals(true, obj.get("obs"));

    obj = (Struct) records.get(1).value();
    assertEquals("0176c7561cf3000000000001001122d4", obj.get("ep"));
    assertEquals("0176c7561cf3000000000001001122d4", obj.get("original_ep"));
    assertEquals("default", obj.get("ept"));

    resources = obj.getArray("resource");
    assertEquals(2, resources.size());

    obj = resources.get(0);
    assertEquals("/10255/0/4", obj.get("path"));
    assertEquals("Class", obj.get("rt"));
    assertEquals("application/vnd.oma.lwm2m+tlv", obj.get("ct"));
    assertEquals(true, obj.get("obs"));

    obj = resources.get(1);
    assertEquals("/10255/0/3", obj.get("path"));
    assertEquals("Vendor", obj.get("rt"));
    assertEquals("", obj.get("ct"));
    assertEquals(true, obj.get("obs"));
  }

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
    assertEquals("200", obj.get("status"));
    assertEquals("600", obj.get("payload"));
    assertEquals("text/plain", obj.get("ct"));
    assertEquals("0", obj.get("max-age"));
  }
}