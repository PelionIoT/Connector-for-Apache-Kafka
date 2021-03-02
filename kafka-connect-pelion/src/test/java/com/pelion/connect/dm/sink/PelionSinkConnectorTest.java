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

import org.apache.kafka.common.config.Config;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class PelionSinkConnectorTest {

  private PelionSinkConnector connector;
  private Map<String, String> props;

  @Before
  public void setup() {
    props = new HashMap<>();
    props.put(PelionSinkConnectorConfig.PELION_API_HOST_CONFIG, "api.us-east-1.mbedcloud.com");
    props.put(PelionSinkConnectorConfig.PELION_ACCESS_KEY_CONFIG, "key1, key2");

    connector = new PelionSinkConnector();
  }

  @Test(expected = ConnectException.class)
  public void shouldCatchInvalidConfigs() {
    connector.start(new HashMap<>());
  }

  @Test
  public void shouldGenerateValidTaskConfigs() {
    connector.start(props);
    List<Map<String, String>> taskConfigs = connector.taskConfigs(2);
    assertFalse("zero task configs provided", taskConfigs.isEmpty());
    for (Map<String, String> taskConfig : taskConfigs) {
      assertEquals(props, taskConfig);
    }
  }

  @Test
  public void shouldNotHaveNullConfigDef() {
    assertNotNull(connector.config());
  }

  @Test
  public void shouldReturnConnectorType() {
    assertTrue(SinkConnector.class.isAssignableFrom(connector.getClass()));
  }

  @Test
  public void shouldReturnSinkTask() {
    assertEquals(PelionSinkTask.class, connector.taskClass());
  }

  @Test
  public void shouldStartAndStop() {
    connector.start(props);
    connector.stop();
  }

  @Test
  public void shouldValidateConfigs() {
    Config result = connector.validate(props);
    assertNotNull(result);
    result.configValues()
        .stream()
        .forEach(config -> assertTrue(config.errorMessages().isEmpty()));
  }

  @Test
  public void testVersion() {
    assertNotNull(connector.version());
    assertNotEquals("0.0.0.0", connector.version());
    assertNotEquals("unknown", connector.version());
    assertTrue(connector.version().matches("^(\\d+\\.){2}?(\\*|\\d+)(-.*)?$"));
  }
}
