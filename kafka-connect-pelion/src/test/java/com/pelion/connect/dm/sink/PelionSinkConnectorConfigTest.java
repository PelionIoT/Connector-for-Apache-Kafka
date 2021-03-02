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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class PelionSinkConnectorConfigTest {

  private ConfigDef configDef = PelionSinkConnectorConfig.config();
  private Map<String, String> props;

  @Before
  public void setup() {
    props = new HashMap<>();
    props.put(PelionSinkConnectorConfig.PELION_API_HOST_CONFIG, "api.us-east-1.mbedcloud.com");
    props.put(PelionSinkConnectorConfig.PELION_ACCESS_KEY_CONFIG, "key1, key2");
  }

  @Test
  public void doc() {
    System.out.println(PelionSinkConnectorConfig.config().toRst());
  }

  @Test(expected = ConfigException.class)
  public void shouldFailIfAccessKeyIsNotDefined() {
    new PelionSinkConnectorConfig(new HashMap<>());
  }

  @Test
  public void initialConfigIsValid() {
    assertTrue(configDef.validate(props)
        .stream()
        .allMatch(configValue -> configValue.errorMessages().size() == 0));
  }
}
